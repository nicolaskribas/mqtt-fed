use crate::{federator::Context, message::CoreAnn};
use std::sync::Arc;
use tokio::{task, time};
use tracing::info;

pub(crate) struct Announcer {
    federated_topic: Arc<String>, // for logging the stop
    announcement_loop: task::JoinHandle<()>,
}

impl Announcer {
    pub(crate) fn new(federated_topic: Arc<String>, ctx: Arc<Context>) -> Self {
        let mut interval = time::interval(ctx.core_ann_interval);
        let mut ann = CoreAnn {
            core_id: ctx.id,
            seqn: 0,
            dist: 0,
            sender_id: ctx.id,
        };

        let topic_clone = federated_topic.clone();

        let announcement_loop = tokio::spawn(async move {
            loop {
                interval.tick().await;

                let core_ann = ann.serialize(&topic_clone);

                let neighbors = ctx.neighbors.read().await;
                for neighbor in neighbors.values() {
                    neighbor.publish(core_ann.clone());
                }

                ann.seqn += 1;
            }
        });
        info!(%federated_topic, "started announcing as core");

        Announcer {
            federated_topic,
            announcement_loop,
        }
    }
}

impl Drop for Announcer {
    fn drop(&mut self) {
        self.announcement_loop.abort();
        info!(federated_topic = %self.federated_topic, "stopped announcing as core");
    }
}
