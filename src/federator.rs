use crate::{
    conf::{BrokerConfig, FederatorConfig},
    message::{self, Message, BEACONS, CORE_ANNS, FEDERATED_TOPICS, MEMB_ANNS, ROUTING_TOPICS},
    worker::TopicWorkerHandle,
};
use paho_mqtt as mqtt;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::RwLock;
use tracing::{error, info, warn};

const BUFFER_SIZE: usize = 256;
const HOST_QOS: i32 = 2;
pub(crate) const NEIGHBORS_QOS: i32 = 2;

pub(crate) type Id = u32;

pub(crate) struct Context {
    pub(crate) id: Id,
    pub(crate) core_ann_interval: Duration,
    pub(crate) beacon_interval: Duration,
    pub(crate) redundancy: usize,
    pub(crate) cache_size: usize,
    pub(crate) neighbors: RwLock<HashMap<Id, mqtt::AsyncClient>>,
    pub(crate) host_client: mqtt::AsyncClient,
}

pub(crate) struct Federator {
    ctx: Arc<Context>,
    workers: HashMap<String, TopicWorkerHandle>,
    msg_stream: mqtt::AsyncReceiver<Option<mqtt::Message>>,
}

pub(crate) fn run(config: FederatorConfig) -> Result<(), ()> {
    info!(?config, "starting federator");

    let neighbors_clients = create_neighbors_clients(config.neighbors);

    let mut host_client = new_host_client(config.host.uri)
        .map_err(|err| error!("error creating mqtt client for host broker: {err}"))?;

    let rx = host_client.get_stream(BUFFER_SIZE);

    let ctx = Context {
        id: config.host.id,
        core_ann_interval: Duration::from_secs(config.core_ann_interval),
        beacon_interval: Duration::from_secs(config.beacon_interval),
        redundancy: config.redundancy,
        neighbors: RwLock::new(neighbors_clients),
        cache_size: config.cache_size,
        host_client,
    };

    let mut federator = Federator {
        ctx: Arc::new(ctx),
        workers: HashMap::new(),
        msg_stream: rx,
    };

    new_runtime().block_on(federator.run())
}

impl Federator {
    pub(crate) async fn run(&mut self) -> Result<(), ()> {
        self.connect().await?;

        // start receiving messages
        while let Ok(next) = self.msg_stream.recv().await {
            if let Some(mqtt_msg) = next {
                if let Ok((federated_topic, message)) = message::deserialize(&mqtt_msg) {
                    if let Some(worker) = self.workers.get(federated_topic) {
                        worker.dispatch(message);
                    } else if matches!(message, Message::Beacon | Message::CoreAnn(_)) {
                        let worker = TopicWorkerHandle::new(federated_topic, self.ctx.clone());
                        worker.dispatch(message);
                        self.workers.insert(federated_topic.to_owned(), worker);
                    }
                }
            } else {
                warn!("got disconnected from host broker");
            }
        }

        Ok(())
    }

    async fn connect(&self) -> Result<(), ()> {
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
            .await
            .map_err(|err| error!("error connecting to host broker: {err}"))?;

        info!("connected to host broker");

        self.ctx
            .host_client
            .subscribe_many_with_options(&topics, &qoss, &opts, None)
            .await
            .map_err(|err| error!(?topics, "error subscribing to topics on host broker: {err}"))?;

        info!(?topics, "subscribed to topics on host broker");

        // connect to neighboring brokers
        for (id, client) in self.ctx.neighbors.read().await.iter() {
            if let Err(err) = client.connect(neighbor_client_conn_opts()).await {
                warn!(id, "cannot connect with neighboring broker: {err}");
            } else {
                info!(id, "connected to neighboring broker");
            }
        }

        Ok(())
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
        .max_buffered_messages(25)
        .mqtt_version(mqtt::MQTT_VERSION_5)
        .restore_messages(false)
        .finalize();

    mqtt::AsyncClient::new(opts)
}

fn create_neighbors_clients(configs: Vec<BrokerConfig>) -> HashMap<u32, mqtt::AsyncClient> {
    let mut neighbors = HashMap::new();

    for ngbr_conf in configs {
        match new_neighbor_client(&ngbr_conf.uri) {
            Ok(client) => {
                neighbors.insert(ngbr_conf.id, client);
            }
            Err(err) => error!(
                ?ngbr_conf,
                "cannot create client for neighbor broker: {}", err
            ),
        }
    }

    neighbors
}

fn new_neighbor_client(uri: &str) -> Result<mqtt::AsyncClient, mqtt::Error> {
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
