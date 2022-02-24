use crate::{
    announcer::Announcer,
    federator::{Context, Id},
    message::{CoreAnn, MeshMembAnn, Message, PubId, RoutedPub, FEDERATED_TOPICS_LEVEL},
};
use lru::LruCache;
use paho_mqtt as mqtt;
use std::{
    cmp::Ordering,
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::mpsc;
use tracing::{info, trace, warn};

pub(crate) struct TopicHandler {
    topic: String,
    ctx: Arc<Context>,
    rx: mpsc::Receiver<Message>,
    cache: LruCache<PubId, ()>,
    next_id: u32,
    latest_beacon: Option<Instant>,
    current_core: Option<Core>,
    children: HashMap<Id, Instant>,
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

impl TopicHandler {
    pub(crate) fn new(topic: String, ctx: Arc<Context>, rx: mpsc::Receiver<Message>) -> Self {
        Self {
            children: HashMap::new(),
            cache: LruCache::new(ctx.cache_size),
            latest_beacon: None,
            current_core: None,
            next_id: 0,
            topic,
            ctx,
            rx,
        }
    }

    pub(crate) async fn start(&mut self) {
        while let Some(message) = self.rx.recv().await {
            match message {
                Message::RoutedPub(routing_pub) => self.handle_routed_pub(routing_pub).await,
                Message::FederatedPub(message) => self.handle_publication(message).await,
                Message::CoreAnn(core_ann) => self.handle_core_ann(core_ann).await,
                Message::MeshMembAnn(memb_ann) => self.handle_memb_ann(memb_ann).await,
                Message::Beacon => self.handle_beacon().await,
            }
        }
    }

    async fn handle_routed_pub(&mut self, routed_pub: RoutedPub) {
        info!("received pub being routed: {:?}", routed_pub);

        if self.cache.contains(&routed_pub.id) {
            return;
        }

        self.cache.put(routed_pub.id, ());

        if self.has_local_sub() {
            let message = mqtt::MessageBuilder::new()
                .payload(&*routed_pub.payload)
                .topic(format!("{}{}", FEDERATED_TOPICS_LEVEL, self.topic))
                .finalize();

            self.ctx.host_client.publish(message);
        }

        if let Some(core) = filter_valid(self.current_core.as_mut(), self.ctx.core_ann_interval) {
            let routing_pub = routed_pub.serialize(&self.topic);

            if let Core::Other(core) = core {
                let ngbr = self.ctx.neighbours.read().await;
                for parent in &core.parents {
                    if let Some(ngbr_client) = ngbr.get(&parent.id) {
                        ngbr_client.publish(routing_pub.clone());
                    }
                }
            }

            let ngbr = self.ctx.neighbours.read().await;
            for (id, time) in &self.children {
                if time.elapsed() < 3 * self.ctx.core_ann_interval {
                    if let Some(client) = ngbr.get(id) {
                        client.publish(routing_pub.clone());
                    }
                }
            }
        }
    }

    // #[instrument(skip_all)]
    async fn handle_publication(&mut self, message: mqtt::Message) {
        info!("client published a message");
        if let Some(core) = filter_valid(self.current_core.as_mut(), self.ctx.core_ann_interval) {
            let new_id = PubId {
                origin: self.ctx.id,
                seqn: self.next_id,
            };

            self.next_id += 1;

            let routed_pub = RoutedPub {
                id: new_id,
                payload: message.payload().to_owned(),
            }
            .serialize(&self.topic);

            self.cache.put(new_id, ());

            if let Core::Other(core) = core {
                let ngbr = self.ctx.neighbours.read().await;
                for parent in &core.parents {
                    if let Some(ngbr_client) = ngbr.get(&parent.id) {
                        ngbr_client.publish(routed_pub.clone());
                    }
                }
            }

            let ngbr = self.ctx.neighbours.read().await;
            for (id, time) in &self.children {
                if time.elapsed() < 3 * self.ctx.core_ann_interval {
                    if let Some(client) = ngbr.get(id) {
                        client.publish(routed_pub.clone());
                    }
                }
            }
        }
    }

    // #[instrument(skip(self))]
    async fn handle_core_ann(&mut self, core_ann: CoreAnn) {
        trace!("handling received core ann");

        if core_ann.core_id == self.ctx.id || core_ann.sender_id == self.ctx.id {
            return;
        }

        if let Some(core) = filter_valid(self.current_core.as_mut(), self.ctx.core_ann_interval) {
            let current_core_id = match core {
                Core::Myself(_) => self.ctx.id,
                Core::Other(CoreBroker { id, .. }) => *id,
            };

            if core_ann.core_id == current_core_id {
                if let Core::Other(core) = core {
                    match (
                        core_ann.seqn.cmp(&core.latest_seqn),
                        (core_ann.dist + 1).cmp(&core.dist),
                    ) {
                        (Ordering::Greater, _) | (Ordering::Equal, Ordering::Less) => {
                            core.latest_seqn = core_ann.seqn;
                            core.dist = core_ann.dist + 1;
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

                            info!(
                                "seqn: {}, dist: {}, parents: {:?}",
                                core.latest_seqn,
                                core.dist,
                                core.parents.iter().map(|p| p.id).collect::<Vec<_>>()
                            );
                            self.forward(&core_ann).await;
                        }
                        (Ordering::Equal, Ordering::Equal) => {
                            if let Err(pos) = core
                                .parents
                                .binary_search_by(|p| p.id.cmp(&core_ann.sender_id))
                            {
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

                                    info!(
                                        "seqn: {}, dist: {}, parents: {:?}",
                                        core.latest_seqn,
                                        core.dist,
                                        core.parents.iter().map(|p| p.id).collect::<Vec<_>>()
                                    );
                                }
                            };
                        }
                        (Ordering::Less, _) | (Ordering::Equal, Ordering::Greater) => (), // old seqn or longer distance: do nothing
                    }
                } else {
                    panic!("this is an error");
                }
            } else if core_ann.core_id < current_core_id {
                info!(
                    "broker {} is the new core, old core was: {}",
                    core_ann.core_id, current_core_id
                );
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
                    dist: core_ann.dist + 1,
                    has_unanswered_parents: !was_answered,
                });

                self.current_core = Some(new_core);

                self.forward(&core_ann).await;
            }
        } else {
            info!("broker {} is the new core", core_ann.core_id);
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
                dist: core_ann.dist + 1,
                has_unanswered_parents: !was_answered,
            });

            self.current_core = Some(new_core);

            self.forward(&core_ann).await;
        }
    }

    // #[instrument(skip(self))]
    async fn handle_memb_ann(&mut self, memb_ann: MeshMembAnn) {
        if memb_ann.sender_id == self.ctx.id {
            return;
        }

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
                        info!(
                            "children: {:?}",
                            self.children
                                .iter()
                                .filter(|(_, time)| time.elapsed() < 3 * self.ctx.core_ann_interval)
                                .map(|(id, _)| id)
                                .collect::<Vec<_>>()
                        );
                        ()
                    }
                    Core::Other(core) => {
                        if memb_ann.seqn == core.latest_seqn {
                            self.children.insert(memb_ann.sender_id, Instant::now());
                            answer_parents(core, &self.ctx, &self.topic).await;
                            info!(
                                "children: {:?}",
                                self.children
                                    .iter()
                                    .filter(
                                        |(_, time)| time.elapsed() < 3 * self.ctx.core_ann_interval
                                    )
                                    .map(|(id, _)| id)
                                    .collect::<Vec<_>>()
                            );
                        }
                    }
                }
            }
        }
    }

    // #[instrument(skip(self))]
    async fn handle_beacon(&mut self) {
        trace!("recived beacon");

        self.latest_beacon = Some(Instant::now());

        if let Some(core) = filter_valid(self.current_core.as_mut(), self.ctx.core_ann_interval) {
            if let Core::Other(core) = core {
                answer_parents(core, &self.ctx, &self.topic).await;
            }
        } else {
            let announcer = Announcer::new(&self.topic, &self.ctx);
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

        let ngbrs = self.ctx.neighbours.read().await;
        for (_, ngbr_client) in ngbrs.iter().filter(|(&id, _)| id != core_ann.sender_id) {
            ngbr_client.publish(my_core_ann.clone());
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
        .serialize(&topic);

        let ngbrs = ctx.neighbours.read().await;
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

    let ngbrs = ctx.neighbours.read().await;
    if let Some(sender_client) = ngbrs.get(&core_ann.sender_id) {
        sender_client.publish(my_memb_ann);
    } else {
        warn!("{} is not a neighbour", core_ann.sender_id);
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
