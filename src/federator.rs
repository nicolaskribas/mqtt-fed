use crate::{
    conf::{BrokerConfig, FederatorConfig},
    handler::TopicHandler,
    message::{self, Message, BEACONS, CORE_ANNS, FEDERATED_TOPICS, MEMB_ANNS, ROUTING_TOPICS},
};
use paho_mqtt as mqtt;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::{
    mpsc::{self, error::TrySendError, Sender},
    RwLock,
};
use tracing::{error, info, warn};

const BUFFER_SIZE: usize = 256;
const HANDLER_BUFFER_SIZE: usize = 64;
const HOST_QOS: i32 = 2;
pub(crate) const NEIGHBOURS_QOS: i32 = 2;

pub(crate) type Id = u32;

pub(crate) struct Context {
    pub(crate) id: Id,
    pub(crate) core_ann_interval: Duration,
    pub(crate) beacon_interval: Duration,
    pub(crate) redundancy: usize,
    pub(crate) cache_size: usize,
    pub(crate) neighbours: RwLock<HashMap<Id, mqtt::AsyncClient>>,
    pub(crate) host_client: mqtt::AsyncClient,
}

pub(crate) struct Federator {
    ctx: Arc<Context>,
    handlers: HashMap<String, mpsc::Sender<Message>>,
    msg_stream: mqtt::AsyncReceiver<Option<mqtt::Message>>,
}

pub(crate) fn run(conf: FederatorConfig) -> Result<(), mqtt::Error> {
    info!(
        "Starting federator {}. Redundancy: {}, Beacon interval: {}s, Core announcements interval: {}s.",
        conf.host.id, conf.redundancy, conf.beacon_interval, conf.core_ann_interval
    );

    let neighbors_clients = create_neigbours_clients(conf.neighbours);

    let mut host_client = new_host_client(conf.host.uri)?;
    let rx = host_client.get_stream(BUFFER_SIZE);

    let ctx = Context {
        id: conf.host.id,
        core_ann_interval: Duration::from_secs(conf.core_ann_interval),
        beacon_interval: Duration::from_secs(conf.beacon_interval),
        redundancy: conf.redundancy,
        neighbours: RwLock::new(neighbors_clients),
        cache_size: conf.cache_size,
        host_client,
    };

    let mut federator = Federator {
        ctx: Arc::new(ctx),
        handlers: HashMap::new(),
        msg_stream: rx,
    };

    new_runtime().block_on(federator.run());
    Ok(())
}

impl Federator {
    pub(crate) async fn run(&mut self) -> Result<(), mqtt::Error> {
        self.connect().await?;

        // start receiving messages
        while let Ok(next) = self.msg_stream.recv().await {
            if let Some(mqtt_msg) = next {
                if let Ok((topic, message)) = message::deserialize(mqtt_msg) {
                    if let Some(handler) = self.handlers.get(&topic) {
                        match handler.try_send(message) {
                            Err(TrySendError::Full(_)) => {
                                warn!("channel is full, droping packet for \"{topic}\"")
                            }
                            Err(TrySendError::Closed(_)) => {
                                error!("channel for \"{topic}\" was closed")
                            }
                            Ok(_) => (),
                        }
                    } else if message.is_beacon() || message.is_core_ann() {
                        let handler = self.spawn_handler_for(&topic);

                        match handler.try_send(message) {
                            Err(TrySendError::Full(_)) => {
                                warn!("channel is full, droping message for \"{topic}\"")
                            }
                            Err(TrySendError::Closed(_)) => {
                                error!("channel for \"{topic}\" was closed")
                            }
                            Ok(_) => (),
                        }

                        self.handlers.insert(topic.to_owned(), handler);
                    }
                }
            } else {
                warn!("got disconnected from host broker");
            }
        }

        Ok(())
    }

    async fn connect(&self) -> Result<(), mqtt::Error> {
        // connect to host broker
        let topics = vec![
            CORE_ANNS,
            MEMB_ANNS,
            ROUTING_TOPICS,
            FEDERATED_TOPICS,
            BEACONS,
        ];
        let opts = vec![mqtt::SubscribeOptions::new(mqtt::SUBSCRIBE_NO_LOCAL); topics.len()];
        let qoss = vec![HOST_QOS; topics.len()];

        self.ctx
            .host_client
            .connect(host_client_conn_opts())
            .await?;
        info!("connected to host broker");

        self.ctx
            .host_client
            .subscribe_many_with_options(&topics, &qoss, &opts, None)
            .await?;
        info!("subscribed to {:?}", topics);

        // connect to neighbouring brokers
        for (id, client) in self.ctx.neighbours.read().await.iter() {
            if let Err(err) = client.connect(neighbor_client_conn_opts()).await {
                warn!("cannot connect with neighboring broker {id}: {err}");
            } else {
                info!("connected to neighboring broker {id}");
            }
        }

        Ok(())
    }

    fn spawn_handler_for(&self, topic: &str) -> Sender<message::Message> {
        info!("spawning new handler for \"{topic}\"");

        let (tx, rx) = mpsc::channel(HANDLER_BUFFER_SIZE);

        let mut handler = TopicHandler::new(topic.to_owned(), self.ctx.clone(), rx);
        tokio::spawn(async move { handler.start().await });

        tx
    }
}

fn new_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_time()
        .build()
        .unwrap()
}

fn new_host_client(uri: String) -> Result<mqtt::AsyncClient, mqtt::Error> {
    let opts = mqtt::CreateOptionsBuilder::new()
        .server_uri(uri)
        .persistence(None)
        .max_buffered_messages(0)
        .mqtt_version(mqtt::MQTT_VERSION_5)
        .restore_messages(false)
        .finalize();

    mqtt::AsyncClient::new(opts)
}

fn create_neigbours_clients(configs: Vec<BrokerConfig>) -> HashMap<u32, mqtt::AsyncClient> {
    let mut neighbours = HashMap::new();

    for ngbr_conf in configs {
        match new_neighbour_client(ngbr_conf.uri) {
            Ok(client) => {
                neighbours.insert(ngbr_conf.id, client);
                ()
            }
            Err(err) => error!(
                "cannot create client for neighbour broker {}: {}",
                ngbr_conf.id, err
            ),
        }
    }

    neighbours
}

fn new_neighbour_client(uri: String) -> Result<mqtt::AsyncClient, mqtt::Error> {
    let opts = mqtt::CreateOptionsBuilder::new()
        .server_uri(uri)
        .persistence(None)
        .max_buffered_messages(25)
        .mqtt_version(mqtt::MQTT_VERSION_DEFAULT)
        .restore_messages(false)
        .delete_oldest_messages(true)
        .finalize();

    mqtt::AsyncClient::new(opts)
}

fn host_client_conn_opts() -> mqtt::ConnectOptions {
    mqtt::ConnectOptionsBuilder::new()
        .keep_alive_interval(Duration::from_secs(60))
        .clean_session(true)
        .automatic_reconnect(Duration::from_secs(1), Duration::from_secs(60))
        .finalize()
}

fn neighbor_client_conn_opts() -> mqtt::ConnectOptions {
    mqtt::ConnectOptionsBuilder::default()
        .clean_session(true)
        .finalize()
}
