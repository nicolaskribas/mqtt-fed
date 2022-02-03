use crate::announcer::Announcer;
use crate::federator::Context;
use crate::federator::Id;
use crate::message::{CoreAnn, MeshMembAnn, Message};
use crate::parents::CoreAnnCollection;
use paho_mqtt as mqtt;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{info, instrument};

pub(crate) struct Handler {
    topic: String,
    ctx: Arc<Context>,
    rx: mpsc::Receiver<Message>,
    latest_beacon: Option<Instant>,
    mesh: Option<Mesh>,
}

impl Handler {
    pub(crate) fn new(topic: String, ctx: Arc<Context>, rx: mpsc::Receiver<Message>) -> Self {
        Self {
            topic,
            ctx,
            rx,
            latest_beacon: None,
            mesh: None,
        }
    }

    pub(crate) async fn start(&mut self) {
        while let Some(message) = self.rx.recv().await {
            match message {
                Message::FederatedPub(message) => self.handle_publication(message).await,
                Message::CoreAnn(core_ann) => self.handle_core_ann(core_ann).await,
                Message::MeshMembAnn(memb_ann) => self.handle_memb_ann(memb_ann).await,
                Message::Beacon => self.handle_beacon().await,
            }
        }
    }

    #[instrument(skip_all)]
    async fn handle_publication(&self, _message: mqtt::Message) {
        info!("handling received pubication");
        info!("not routing yet");
    }

    #[instrument(skip(self))]
    async fn handle_core_ann(&mut self, core_ann: CoreAnn) {
        info!("handling received core ann");

        if core_ann.core_id == self.ctx.id {
            return;
        }

        let has_local_sub = self.has_local_sub();

        if let Some(mesh) = validate_mesh_mut(&mut self.mesh, self.ctx.core_ann_interval) {
            let current_id = match mesh.core {
                Core::Local(_) => self.ctx.id,
                Core::Remote { id, .. } => id,
            };

            if core_ann.core_id == current_id {
                if let Core::Remote { core_anns, id } = &mut mesh.core {
                    let prev_seqn = core_anns.get_latest_seqn();
                    if let Ok(_) = core_anns.insert(&core_ann) {
                        if core_anns.get_latest_seqn() > prev_seqn {
                            // new seq number, route core ann to neighbours
                            let core_ann_to_route = CoreAnn {
                                dist_to_core: core_anns.get_min_dist(),
                                sender_id: self.ctx.id,
                                core_id: *id,
                                seq_number: core_anns.get_latest_seqn(),
                            };

                            let ann = core_ann_to_route.serialize(&self.topic);

                            let neighbours = self.ctx.neighbours.read().await;

                            for (_, nbr) in neighbours
                                .iter()
                                .filter(|(&id, _)| id != core_ann.sender_id)
                            {
                                nbr.publish(ann.clone());
                            }
                        }

                        if has_local_sub && core_anns.is_parent(core_ann.sender_id) {
                            let memb_ann = MeshMembAnn {
                                core_id: *id,
                                seq_number: core_ann.seq_number,
                                sender_id: self.ctx.id,
                            };

                            let neighbours = self.ctx.neighbours.read().await;

                            if let Some(client) = neighbours.get(&core_ann.sender_id) {
                                client.publish(memb_ann.serialize(&self.topic));
                            }
                        }
                    }
                } else {
                }
            } else if core_ann.core_id < current_id {
                self.acknowledge_new_core(core_ann).await;
            }
        } else {
            self.acknowledge_new_core(core_ann).await;
        }
    }

    #[instrument(skip(self))]
    async fn handle_memb_ann(&mut self, memb_ann: MeshMembAnn) {
        info!("handling new memb ann");

        let neighbours = self.ctx.neighbours.read().await;
        if !neighbours.contains_key(&memb_ann.sender_id) {
            info!(
                "received a mesh membership announcement from a non neighbour broker id: {}",
                memb_ann.sender_id
            );
            return;
        }

        if let Some(mesh) = validate_mesh_mut(&mut self.mesh, self.ctx.core_ann_interval) {
            let current_id = match mesh.core {
                Core::Local(_) => self.ctx.id,
                Core::Remote { id, .. } => id,
            };

            if current_id == memb_ann.core_id {
                mesh.children.insert(memb_ann.sender_id, Instant::now());

                if let Core::Remote { core_anns, id } = &mut mesh.core {
                    let neighbours_client = self.ctx.neighbours.read().await;
                    for parent_entry in core_anns.get_parents().filter(|pe| !pe.was_answered()) {
                        if let Some(client) = neighbours_client.get(&parent_entry.get_id()) {
                            let memb_ann = MeshMembAnn {
                                core_id: *id,
                                seq_number: parent_entry.get_seq_number(),
                                sender_id: self.ctx.id,
                            };
                            client.publish(memb_ann.serialize(&self.topic));
                        } else {
                            info!(
                                "parent id {} is no longer a neighbour",
                                parent_entry.get_id()
                            );
                        }
                        parent_entry.set_answered();
                    }
                }
            }
        }
    }

    #[instrument(skip(self))]
    async fn handle_beacon(&mut self) {
        info!("recived beacon");
        self.latest_beacon = Some(Instant::now());

        if let Some(mesh) = validate_mesh_mut(&mut self.mesh, self.ctx.core_ann_interval) {
            if let Core::Remote { core_anns, id } = &mut mesh.core {
                let neighbours_client = self.ctx.neighbours.read().await;
                for parent_entry in core_anns.get_parents().filter(|pe| !pe.was_answered()) {
                    if let Some(client) = neighbours_client.get(&parent_entry.get_id()) {
                        let memb_ann = MeshMembAnn {
                            core_id: *id,
                            seq_number: parent_entry.get_seq_number(),
                            sender_id: self.ctx.id,
                        };
                        client.publish(memb_ann.serialize(&self.topic));
                    } else {
                        info!(
                            "parent id {} is no longer a neighbour",
                            parent_entry.get_id()
                        );
                    }
                    parent_entry.set_answered();
                }
            }
        } else {
            let announcer = Announcer::new(&self.topic, &self.ctx);
            self.mesh = Some(Mesh {
                core: Core::Local(announcer),
                children: HashMap::new(),
            });
        }
    }

    async fn acknowledge_new_core(&mut self, core_ann: CoreAnn) {
        info!("broker with id {} elected as new core", core_ann.core_id);

        let core_anns = CoreAnnCollection::new(&self.ctx, &core_ann);

        let core = Core::Remote {
            core_anns,
            id: core_ann.core_id,
        };

        self.mesh = Some(Mesh {
            core,
            children: HashMap::new(),
        });

        if self.has_local_sub() {
            let memb_ann = MeshMembAnn {
                core_id: core_ann.core_id,
                seq_number: core_ann.seq_number,
                sender_id: self.ctx.id,
            };

            let neighbours = self.ctx.neighbours.read().await;

            if let Some(client) = neighbours.get(&core_ann.sender_id) {
                client.publish(memb_ann.serialize(&self.topic));
            }
        }

        let core_ann_to_route = CoreAnn {
            dist_to_core: core_ann.dist_to_core + 1,
            sender_id: self.ctx.id,
            ..core_ann
        };

        let ann = core_ann_to_route.serialize(&self.topic);

        let neighbours = self.ctx.neighbours.read().await;

        for (_, nbr) in neighbours
            .iter()
            .filter(|(&id, _)| id != core_ann.sender_id)
        {
            nbr.publish(ann.clone());
        }
    }

    fn has_local_sub(&self) -> bool {
        if let Some(time) = self.latest_beacon {
            time.elapsed() < 3 * self.ctx.beacon_interval
        } else {
            false
        }
    }
}

struct Mesh {
    core: Core,
    children: HashMap<Id, Instant>,
}

enum Core {
    Local(Announcer),
    Remote {
        id: Id,
        core_anns: CoreAnnCollection,
    },
}

fn validate_mesh_mut(mesh: &mut Option<Mesh>, core_ann_timeout: Duration) -> Option<&mut Mesh> {
    mesh.as_mut().and_then(|mesh| match &mesh.core {
        Core::Local(_) => Some(mesh),
        Core::Remote { core_anns, .. } => {
            if core_anns.latest().elapsed() < 3 * core_ann_timeout {
                Some(mesh)
            } else {
                None // timeout reached, the mesh is considered as not existing anymore
            }
        }
    })
}
