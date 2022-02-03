use crate::conf::FederatorConfig;
use crate::handler::Handler;
use crate::message::{self, Message};
use crate::topic::{BEACONS, CORE_ANNS, FEDERATED_TOPICS, MEMB_ANNS};
use mqtt::ConnectOptionsBuilder;
use paho_mqtt as mqtt;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{error::TrySendError, Sender};
use tokio::sync::{mpsc, RwLock};
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
    pub(crate) neighbours: RwLock<HashMap<Id, mqtt::AsyncClient>>,
}

pub(crate) struct Federator {
    host_client: mqtt::AsyncClient,
    ctx: Arc<Context>,
    handlers: HashMap<String, mpsc::Sender<Message>>,
}

pub(crate) fn run(conf: FederatorConfig) -> Result<(), mqtt::Error> {
    let client_id = format!("federator-{id}", id = conf.host.id);

    let mut neighbours = HashMap::new();
    for neighbour in conf.neighbours {
        let client = create_neighbour_client(neighbour.uri, &client_id)?;
        neighbours.insert(neighbour.id, client);
    }

    let ctx = Context {
        id: conf.host.id,
        core_ann_interval: Duration::from_secs(conf.core_ann_interval),
        beacon_interval: Duration::from_secs(conf.beacon_interval),
        redundancy: conf.redundancy,
        neighbours: RwLock::new(neighbours),
    };

    let mut federator = Federator {
        host_client: new_host_client(conf.host.uri, &client_id)?,
        ctx: Arc::new(ctx),
        handlers: HashMap::new(),
    };

    let rt = new_runtime();
    rt.block_on(federator.run());

    Ok(())
}

impl Federator {
    pub(crate) async fn run(&mut self) {
        // get a channel of messages arriving in the host client
        let (tx, mut rx) = mpsc::channel(BUFFER_SIZE);
        self.host_client.set_message_callback(move |_, mqtt_msg| {
            let mqtt_msg = mqtt_msg.expect("no None message will be passed to the callback");

            if let Err(err) = tx.try_send(mqtt_msg) {
                match err {
                    TrySendError::Full(_) => {
                        warn!("channel is full, host client dropping messages")
                    }
                    TrySendError::Closed(_) => error!("host client channel was closed"),
                }
            }
        });

        // connections and subscriptions
        self.connect_host_client().await;
        self.subscribe_to_topics().await;
        self.connect_neighbours().await;

        // start receiving messages
        while let Some(mqtt_msg) = rx.recv().await {
            match message::deserialize(mqtt_msg) {
                Ok((topic, message)) => {
                    if !self.handlers.contains_key(&topic) {
                        let tx = self.spawn_handler_for(&topic);
                        self.handlers.insert(topic.clone(), tx);
                    }

                    let handler = self.handlers.get(&topic).unwrap();

                    if let Err(err) = handler.try_send(message) {
                        match err {
                            TrySendError::Full(_) => {
                                warn!("channel is full, droping message for \"{topic}\"")
                            }
                            TrySendError::Closed(_) => error!("channel for \"{topic}\" was closed"),
                        }
                    }
                }
                Err(err) => info!("unable to deserialize message: {err}"),
            }
        }
    }

    async fn connect_host_client(&self) -> Result<(), mqtt::Error> {
        let conn_opts = ConnectOptionsBuilder::new()
            .keep_alive_interval(Duration::from_secs(60))
            .clean_session(true)
            .automatic_reconnect(Duration::from_secs(1), Duration::from_secs(60))
            .finalize();

        self.host_client.connect(conn_opts).await?;

        Ok(())
    }

    async fn connect_neighbours(&self) {
        let connect_options = ConnectOptionsBuilder::default()
            .clean_session(true)
            .finalize();

        let neighbours = self.ctx.neighbours.read().await;
        for nbr in neighbours.values() {
            nbr.connect(connect_options.clone());
        }
    }

    async fn subscribe_to_topics(&self) {
        self.host_client
            .subscribe_many(
                &vec![CORE_ANNS, MEMB_ANNS, FEDERATED_TOPICS, BEACONS],
                &vec![HOST_QOS, HOST_QOS, HOST_QOS, HOST_QOS],
            )
            .await;
    }

    fn spawn_handler_for(&self, topic: &str) -> Sender<Message> {
        info!("spawning new handler for \"{topic}\"");

        let (tx, rx) = mpsc::channel(HANDLER_BUFFER_SIZE);

        let mut handler = Handler::new(topic.to_owned(), self.ctx.clone(), rx);
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

fn new_host_client(uri: String, client_id: &str) -> Result<mqtt::AsyncClient, mqtt::Error> {
    let opts = mqtt::CreateOptionsBuilder::new()
        .server_uri(uri)
        .client_id(client_id)
        .persistence(None)
        .max_buffered_messages(0)
        .mqtt_version(mqtt::MQTT_VERSION_DEFAULT)
        .restore_messages(false)
        .finalize();

    let mut client = mqtt::AsyncClient::new(opts)?;

    client.set_connected_callback(|_| info!("connected to host broker"));
    client.set_connection_lost_callback(|_| warn!("connection to host broker lost"));

    Ok(client)
}

fn create_neighbour_client(uri: String, client_id: &str) -> Result<mqtt::AsyncClient, mqtt::Error> {
    let opts = mqtt::CreateOptionsBuilder::new()
        .server_uri(uri)
        .client_id(client_id)
        .persistence(None)
        .max_buffered_messages(25)
        .mqtt_version(mqtt::MQTT_VERSION_DEFAULT)
        .restore_messages(false)
        .delete_oldest_messages(true)
        .finalize();

    let client = mqtt::AsyncClient::new(opts)?;

    Ok(client)
}
