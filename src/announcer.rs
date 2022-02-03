use crate::{federator::Context, message::CoreAnn};

use std::sync::Arc;
use tokio::{task, time};
use tracing::info;

pub(crate) struct Announcer {
    topic: String, // for logging the stop
    announcement_loop: task::JoinHandle<()>,
}

impl Announcer {
    pub(crate) fn new(topic: &str, ctx: &Arc<Context>) -> Self {
        let topic = topic.to_owned();
        let ctx = ctx.to_owned();

        let mut interval = time::interval(ctx.core_ann_interval);
        let mut ann = CoreAnn {
            core_id: ctx.id,
            seq_number: 0,
            dist_to_core: 0,
            sender_id: ctx.id,
        };

        let topic_clone = topic.clone();

        let announcement_loop = tokio::spawn(async move {
            loop {
                interval.tick().await;

                let core_ann = ann.serialize(&topic_clone);

                let neighbours = ctx.neighbours.read().await;
                for neighbour in neighbours.values() {
                    neighbour.publish(core_ann.clone());
                }

                ann.seq_number += 1;
            }
        });

        info!("started announcing as core for {topic}");

        Announcer {
            topic,
            announcement_loop,
        }
    }
}

impl Drop for Announcer {
    fn drop(&mut self) {
        self.announcement_loop.abort();
        info!("stopped announcing as core for {topic}", topic = self.topic);
    }
}
