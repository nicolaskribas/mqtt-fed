use crate::{
    announcer::Announcer,
    federator::{self, Context, Id},
    message::{CoreAnn, FederatedPub, MeshMembAnn, Message, PubId, RoutedPub},
};
use lru::LruCache;
use paho_mqtt as mqtt;
use std::{cmp::Ordering::*, collections::HashMap, sync::Arc, time::Duration};
use tokio::{
    select,
    sync::{
        mpsc::{self, error::TrySendError},
        RwLock,
    },
    time::{sleep_until, Instant},
};
use tracing::{debug, error, info, instrument, trace, warn};

const HANDLER_BUFFER_SIZE: usize = 64;

pub(crate) struct TopicWorkerHandle {
    federated_topic: Arc<String>,
    sender: mpsc::Sender<Message>,
}

impl TopicWorkerHandle {
    pub fn new(federated_topic: &str, ctx: Arc<federator::Context>) -> Self {
        let federated_topic = Arc::new(federated_topic.to_owned());
        let (sender, receiver) = mpsc::channel(HANDLER_BUFFER_SIZE);

        let worker = TopicWorker::new(federated_topic.clone(), ctx, receiver);
        tokio::spawn(start(worker));

        info!(%federated_topic, "spawned new worker");

        Self {
            federated_topic,
            sender,
        }
    }

    pub fn dispatch(&self, message: Message) {
        match self.sender.try_send(message) {
            Err(TrySendError::Full(_)) => {
                warn!(%self.federated_topic, "channel is full, droping message");
            }
            Err(TrySendError::Closed(_)) => {
                error!(%self.federated_topic, "channel was closed");
            }
            Ok(_) => (),
        }
    }
}

pub(crate) struct TopicWorker {
    topic: Arc<String>,
    ctx: Arc<Context>,
    receiver: mpsc::Receiver<Message>,
    cache: LruCache<PubId, ()>,
    next_id: u32,
    latest_beacon: Option<Instant>,
    current_core: Option<Core>,
    children: HashMap<Id, Instant>,
}

pub(crate) async fn start(mut worker: TopicWorker) {
    loop {
        select! {
            biased;

            _ = sub_timeout(worker.latest_beacon, worker.ctx.beacon_interval), if matches!(worker.current_core, Some(Core::Myself(_))) => {
                info!("no more local subs");
                worker.current_core.take();
            }

            maybe_message = worker.receiver.recv() => {
                if let Some(message) = maybe_message{
                    worker.handle(message).await;
                } else {
                    break;
                }
            }
        }
    }
}

async fn sub_timeout(latest: Option<Instant>, beacon_interval: Duration) {
    if let Some(latest) = latest {
        sleep_until(latest + 3 * beacon_interval).await
    }
}

enum Core {
    Myself(Announcer),
    Other(CoreBroker),
}

struct CoreBroker {
    id: Id,
    latest_seqn: u32,
    dist: u32,
    last_heard: Instant,
    parents: Vec<Parent>,
    has_unanswered_parents: bool,
}

#[derive(Debug)]
struct Parent {
    id: Id,
    was_answered: bool,
}

impl TopicWorker {
    pub(crate) fn new(
        topic: Arc<String>,
        ctx: Arc<Context>,
        receiver: mpsc::Receiver<Message>,
    ) -> Self {
        Self {
            children: HashMap::new(),
            cache: LruCache::new(ctx.cache_size),
            latest_beacon: None,
            current_core: None,
            next_id: 0,
            topic,
            ctx,
            receiver,
        }
    }

    async fn handle(&mut self, message: Message) {
        match message {
            Message::RoutedPub(routing_pub) => self.handle_routed_pub(routing_pub).await,
            Message::FederatedPub(federated_pub) => self.handle_publication(federated_pub).await,
            Message::CoreAnn(core_ann) => self.handle_core_ann(core_ann).await,
            Message::MeshMembAnn(memb_ann) => self.handle_memb_ann(memb_ann).await,
            Message::Beacon => self.handle_beacon().await,
        }
    }

    #[instrument(skip_all, fields(federated_topic = %self.topic))]
    async fn handle_routed_pub(&mut self, mut routed_pub: RoutedPub) {
        // check in cache if the message was already routed
        if self.cache.contains(&routed_pub.pub_id) {
            return;
        }
        self.cache.put(routed_pub.pub_id, ());

        // send to local subscribers
        if self.has_local_sub() {
            debug!("sending pub to local subs");

            let federated_pub = FederatedPub {
                payload: routed_pub.payload.clone(),
            }
            .serialize(&self.topic);

            if let Err(err) = self.ctx.host_client.try_publish(federated_pub) {
                warn!("problem creating or queuing message for host broker: {err}");
            }
        }

        if let Some(core) = filter_valid(self.current_core.as_mut(), self.ctx.core_ann_interval) {
            // make myself the sender
            let sender_id = routed_pub.sender_id;
            routed_pub.sender_id = self.ctx.id;

            let routed_pub = routed_pub.serialize(&self.topic);

            // send to mesh parents
            if let Core::Other(core) = core {
                let parents = core
                    .parents
                    .iter()
                    .filter(|p| p.id != sender_id) // except the neighbor i received from
                    .map(|p| &p.id);

                send_to(routed_pub.clone(), parents, &self.ctx.neighbors).await;
            }

            // send to mesh children
            let children = self
                .children
                .iter()
                .filter(|(_, time)| time.elapsed() < 3 * self.ctx.core_ann_interval)
                .filter(|(&id, _)| id != sender_id) // except the neighbor i received from
                .map(|(id, _)| id);

            send_to(routed_pub, children, &self.ctx.neighbors).await;
        }
    }

    #[instrument(skip_all, fields(federated_topic = %self.topic))]
    async fn handle_publication(&mut self, federated_pub: FederatedPub) {
        debug!("client published a message");

        if let Some(core) = filter_valid(self.current_core.as_mut(), self.ctx.core_ann_interval) {
            // new id for the publication
            let new_id = PubId {
                origin_id: self.ctx.id,
                seqn: self.next_id,
            };

            debug!(pub_seqn = new_id.seqn);

            self.next_id += 1;

            let routed_pub = RoutedPub {
                pub_id: new_id,
                payload: federated_pub.payload,
                sender_id: self.ctx.id,
            }
            .serialize(&self.topic);

            // cache the message id to prevent it from being routed twice
            self.cache.put(new_id, ());

            // send to mesh parents
            if let Core::Other(core) = core {
                let parents = core.parents.iter().map(|p| &p.id);

                send_to(routed_pub.clone(), parents, &self.ctx.neighbors).await;
            }

            // send to mesh children
            let children = self
                .children
                .iter()
                .filter(|(_, time)| time.elapsed() < 3 * self.ctx.core_ann_interval)
                .map(|(id, _)| id);

            send_to(routed_pub, children, &self.ctx.neighbors).await;
        }
    }

    #[instrument(skip_all, fields(federated_topic = %self.topic))]
    async fn handle_core_ann(&mut self, mut core_ann: CoreAnn) {
        if core_ann.core_id == self.ctx.id || core_ann.sender_id == self.ctx.id {
            return;
        }

        // takes into account the distance from the neighbor to me
        core_ann.dist += 1;

        if let Some(core) = filter_valid(self.current_core.as_mut(), self.ctx.core_ann_interval) {
            let current_core_id = match core {
                Core::Myself(_) => self.ctx.id,
                Core::Other(CoreBroker { id, .. }) => *id,
            };

            if core_ann.core_id == current_core_id {
                let core = core.unwrap_other();

                // compare the current seqn and dist with the ones in the received core ann
                let seqn_cmp = core_ann.seqn.cmp(&core.latest_seqn);
                let dist_cmp = core_ann.dist.cmp(&core.dist);

                match (seqn_cmp, dist_cmp) {
                    // received a core ann with a diferent distance to the core: because we
                    // are keeping only parents with same distance, the current parents are no
                    // longer valid, so we clean the parents list and add the neighbor from the
                    // receiving core ann as unique parent for now
                    (Greater, _) | (Equal, Less) => {
                        core.latest_seqn = core_ann.seqn;
                        core.dist = core_ann.dist;
                        core.last_heard = Instant::now();

                        let mut was_answered = false;
                        if has_local_sub(&self.latest_beacon, &self.ctx) {
                            answer(&core_ann, &self.topic, &self.ctx).await;
                            was_answered = true;
                        }

                        core.parents.clear();
                        core.parents.push(Parent {
                            id: core_ann.sender_id,
                            was_answered,
                        });

                        core.has_unanswered_parents = !was_answered;

                        self.forward(&core_ann).await;
                    }
                    (Equal, Equal) => {
                        let find_parent = core
                            .parents
                            .binary_search_by(|p| p.id.cmp(&core_ann.sender_id));

                        // neighbor is not already a parent: make it parent if the redundancy
                        // permits or if it has a lower id
                        if let Err(pos) = find_parent {
                            if pos < self.ctx.redundancy {
                                if core.parents.len() == self.ctx.redundancy {
                                    // pop parent with larger id to open room for new parent
                                    core.parents.pop();
                                }

                                let mut was_answered = false;
                                if has_local_sub(&self.latest_beacon, &self.ctx) {
                                    answer(&core_ann, &self.topic, &self.ctx).await;
                                    was_answered = true;
                                }

                                let new_parent = Parent {
                                    id: core_ann.sender_id,
                                    was_answered,
                                };

                                core.parents.insert(pos, new_parent);
                                core.has_unanswered_parents |= !was_answered;
                            }
                        }
                    }

                    // old seqn or longer distance: do nothing
                    (Less, _) | (Equal, Greater) => (),
                }
            } else if core_ann.core_id < current_core_id {
                info!(core_id = current_core_id, "core deposed");
                info!(core_id = core_ann.core_id, "new core elected");

                self.children.clear();

                let mut was_answered = false;
                if self.has_local_sub() {
                    answer(&core_ann, &self.topic, &self.ctx).await;
                    was_answered = true;
                }

                let mut parents = Vec::with_capacity(self.ctx.redundancy);

                parents.push(Parent {
                    id: core_ann.sender_id,
                    was_answered,
                });

                let new_core = Core::Other(CoreBroker {
                    id: core_ann.core_id,
                    parents,
                    latest_seqn: core_ann.seqn,
                    last_heard: Instant::now(),
                    dist: core_ann.dist,
                    has_unanswered_parents: !was_answered,
                });

                self.current_core = Some(new_core);

                self.forward(&core_ann).await;
            }
        } else {
            info!(core_ann.core_id, "new core elected");

            self.children.clear();

            let mut was_answered = false;
            if self.has_local_sub() {
                answer(&core_ann, &self.topic, &self.ctx).await;
                was_answered = true;
            }

            let mut parents = Vec::with_capacity(self.ctx.redundancy);

            parents.push(Parent {
                id: core_ann.sender_id,
                was_answered,
            });

            let new_core = Core::Other(CoreBroker {
                id: core_ann.core_id,
                parents,
                latest_seqn: core_ann.seqn,
                last_heard: Instant::now(),
                dist: core_ann.dist,
                has_unanswered_parents: !was_answered,
            });

            self.current_core = Some(new_core);

            self.forward(&core_ann).await;
        }
    }

    #[instrument(skip(self), fields(federated_topic = %self.topic))]
    async fn handle_memb_ann(&mut self, memb_ann: MeshMembAnn) {
        if memb_ann.sender_id == self.ctx.id {
            return;
        }

        trace!("received a mesh memb ann");

        if let Some(core) = filter_valid(self.current_core.as_mut(), self.ctx.core_ann_interval) {
            let current_core_id = match core {
                Core::Myself(_) => self.ctx.id,
                Core::Other(CoreBroker { id, .. }) => *id,
            };

            if current_core_id == memb_ann.core_id {
                match core {
                    Core::Myself(_) => {
                        // TODO: check seqn of my core anns before adding a child broker
                        self.children.insert(memb_ann.sender_id, Instant::now());
                    }
                    Core::Other(core) => {
                        if memb_ann.seqn == core.latest_seqn {
                            self.children.insert(memb_ann.sender_id, Instant::now());
                            answer_parents(core, &self.ctx, &self.topic).await;
                        }
                    }
                }
            }
        }
    }

    #[instrument(skip_all, fields(federated_topic = %self.topic))]
    async fn handle_beacon(&mut self) {
        trace!("recived beacon");

        self.latest_beacon = Some(Instant::now());

        if let Some(core) = filter_valid(self.current_core.as_mut(), self.ctx.core_ann_interval) {
            if let Core::Other(core) = core {
                answer_parents(core, &self.ctx, &self.topic).await;
            }
        } else {
            let announcer = Announcer::new(self.topic.clone(), self.ctx.clone());
            self.current_core = Some(Core::Myself(announcer));
            self.children.clear();
        }
    }

    fn has_local_sub(&self) -> bool {
        if let Some(time) = self.latest_beacon {
            time.elapsed() < 3 * self.ctx.beacon_interval
        } else {
            false
        }
    }

    async fn forward(&self, core_ann: &CoreAnn) {
        let my_core_ann = CoreAnn {
            dist: core_ann.dist + 1,
            sender_id: self.ctx.id,
            ..*core_ann
        }
        .serialize(&self.topic);

        let ngbrs = self.ctx.neighbors.read().await;
        for (_, ngbr_client) in ngbrs.iter().filter(|(&id, _)| id != core_ann.sender_id) {
            ngbr_client.publish(my_core_ann.clone());
        }
    }
}

async fn send_to(
    message: mqtt::Message,
    mut ids: impl Iterator<Item = &u32>,
    neighbors: &RwLock<HashMap<u32, mqtt::AsyncClient>>,
) {
    if let Some(first_id) = ids.next() {
        let neighbor_clients = neighbors.read().await;

        for id in ids {
            debug!(id, "sending");
            if let Some(client) = neighbor_clients.get(id) {
                if let Err(err) = client.try_publish(message.clone()) {
                    warn!(
                        "problem creating or queuing the message for broker id {id}: {}",
                        err
                    );
                }
            } else {
                debug!("broker {id} is not a neighbor");
            }
        }

        // we reserved an neighbor (the first one in the iterator) so we dont need to do an
        // unecessary clone of the message
        if let Some(client) = neighbor_clients.get(first_id) {
            if let Err(err) = client.try_publish(message) {
                warn!("problem creating or queuing the message: {}", err);
            }
        } else {
            debug!("broker {first_id} is not a neighbor");
        }
    }
}

async fn answer_parents(core: &mut CoreBroker, ctx: &Context, topic: &str) {
    if core.has_unanswered_parents {
        let my_memb_ann = MeshMembAnn {
            core_id: core.id,
            seqn: core.latest_seqn,
            sender_id: ctx.id,
        }
        .serialize(topic);

        let ngbrs = ctx.neighbors.read().await;
        for parent in core.parents.iter_mut().filter(|p| !p.was_answered) {
            if let Some(ngbr_client) = ngbrs.get(&parent.id) {
                ngbr_client.publish(my_memb_ann.clone());
            }
            parent.was_answered = true;
        }
        core.has_unanswered_parents = false;
    }
}

fn has_local_sub(latest_beacon: &Option<Instant>, ctx: &Context) -> bool {
    if let Some(time) = latest_beacon {
        time.elapsed() < 3 * ctx.beacon_interval
    } else {
        false
    }
}

async fn answer(core_ann: &CoreAnn, topic: &str, ctx: &Context) {
    let my_memb_ann = MeshMembAnn {
        core_id: core_ann.core_id,
        seqn: core_ann.seqn,
        sender_id: ctx.id,
    }
    .serialize(topic);

    let ngbrs = ctx.neighbors.read().await;
    if let Some(sender_client) = ngbrs.get(&core_ann.sender_id) {
        sender_client.publish(my_memb_ann);
    } else {
        warn!("{} is not a neighbor", core_ann.sender_id);
    }
}

fn filter_valid(core: Option<&mut Core>, core_ann_interval: Duration) -> Option<&mut Core> {
    core.and_then(|core| match core {
        Core::Myself(_) => Some(core),
        Core::Other(CoreBroker { last_heard, .. }) => {
            if last_heard.elapsed() < 3 * core_ann_interval {
                Some(core)
            } else {
                None
            }
        }
    })
}

impl Core {
    fn unwrap_other(&mut self) -> &mut CoreBroker {
        match self {
            Core::Myself(_) => panic!("unwrap other but the core was myself"),
            Core::Other(core) => core,
        }
    }
}
