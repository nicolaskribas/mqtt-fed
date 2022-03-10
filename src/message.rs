use crate::federator::{Id, NEIGHBORS_QOS};
use core::fmt;
use paho_mqtt as mqtt;
use serde::{Deserialize, Serialize};

pub(crate) const BEACONS: &str = "federator/beacon/#";
const BEACON_TOPIC_LEVEL: &str = "federator/beacon/";

pub(crate) const CORE_ANNS: &str = "federator/core_ann/#";
const CORE_ANN_TOPIC_LEVEL: &str = "federator/core_ann/";

pub(crate) const MEMB_ANNS: &str = "federator/memb_ann/#";
const MEMB_ANN_TOPIC_LEVEL: &str = "federator/memb_ann/";

pub(crate) const FEDERATED_TOPICS: &str = "federated/#";
pub(crate) const FEDERATED_TOPICS_LEVEL: &str = "federated/";

pub(crate) const ROUTING_TOPICS: &str = "federator/routing/#";
const ROUTING_TOPICS_LEVEL: &str = "federator/routing/";

pub(crate) enum Message {
    FederatedPub(FederatedPub),
    RoutedPub(RoutedPub),
    CoreAnn(CoreAnn),
    MeshMembAnn(MeshMembAnn),
    Beacon,
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct RoutedPub {
    pub pub_id: PubId,
    pub sender_id: Id,
    pub payload: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct FederatedPub {
    pub payload: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct CoreAnn {
    pub core_id: u32,
    pub seqn: u32,
    pub dist: u32,
    pub sender_id: u32,
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct MeshMembAnn {
    pub core_id: u32,
    pub seqn: u32,
    pub sender_id: u32,
}

pub(crate) fn deserialize(mqtt_msg: &mqtt::Message) -> Result<(&str, Message), MessageError> {
    let topic = mqtt_msg.topic();

    if let Some(fededarted_topic) = topic.strip_prefix(ROUTING_TOPICS_LEVEL) {
        assert_not_empty(fededarted_topic)?;
        match bincode::deserialize(mqtt_msg.payload()) {
            Ok(routing_pub) => Ok((fededarted_topic, Message::RoutedPub(routing_pub))),
            Err(err) => Err(MessageError::DeserializationError(err)),
        }
    } else if let Some(federated_topic) = topic.strip_prefix(FEDERATED_TOPICS_LEVEL) {
        assert_not_empty(federated_topic)?;
        let federated_pub = FederatedPub {
            payload: mqtt_msg.payload().to_owned(),
        };
        Ok((federated_topic, Message::FederatedPub(federated_pub)))
    } else if let Some(topic) = topic.strip_prefix(CORE_ANN_TOPIC_LEVEL) {
        assert_not_empty(topic)?;
        match bincode::deserialize(mqtt_msg.payload()) {
            Ok(core_ann) => Ok((topic, Message::CoreAnn(core_ann))),
            Err(err) => Err(MessageError::DeserializationError(err)),
        }
    } else if let Some(federated_topic) = topic.strip_prefix(MEMB_ANN_TOPIC_LEVEL) {
        assert_not_empty(federated_topic)?;
        match bincode::deserialize(mqtt_msg.payload()) {
            Ok(memb_ann) => Ok((federated_topic, Message::MeshMembAnn(memb_ann))),
            Err(err) => Err(MessageError::DeserializationError(err)),
        }
    } else if let Some(federated_topic) = topic.strip_prefix(BEACON_TOPIC_LEVEL) {
        assert_not_empty(federated_topic)?;
        Ok((federated_topic, Message::Beacon))
    } else {
        panic!("received a packet from a topic it was not supposed to be subscribed to");
    }
}

impl FederatedPub {
    pub(crate) fn serialize(&self, fed_topic: &str) -> mqtt::Message {
        let topic = format!("{FEDERATED_TOPICS_LEVEL}{fed_topic}");

        mqtt::Message::new(topic, &*self.payload, NEIGHBORS_QOS)
    }
}

impl RoutedPub {
    pub(crate) fn serialize(&self, fed_topic: &str) -> mqtt::Message {
        let topic = format!("{ROUTING_TOPICS_LEVEL}{fed_topic}");
        let payload = bincode::serialize(self).unwrap();

        mqtt::Message::new(topic, payload, NEIGHBORS_QOS)
    }
}

impl CoreAnn {
    pub(crate) fn serialize(&self, fed_topic: &str) -> mqtt::Message {
        let topic = format!("{CORE_ANN_TOPIC_LEVEL}{fed_topic}");
        let payload = bincode::serialize(self).unwrap();

        mqtt::Message::new(topic, payload, NEIGHBORS_QOS)
    }
}

impl MeshMembAnn {
    pub(crate) fn serialize(&self, fed_topic: &str) -> mqtt::Message {
        let topic = format!("{MEMB_ANN_TOPIC_LEVEL}{fed_topic}");
        let payload = bincode::serialize(self).unwrap();

        mqtt::Message::new(topic, payload, NEIGHBORS_QOS)
    }
}

fn assert_not_empty(topic: &str) -> Result<(), MessageError> {
    if topic.is_empty() {
        Err(MessageError::EmptyFederatedTopic)
    } else {
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) enum MessageError {
    EmptyFederatedTopic,
    DeserializationError(bincode::Error),
}

impl fmt::Display for MessageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyFederatedTopic => "federated topic is empty".fmt(f),
            Self::DeserializationError(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for MessageError {}

#[derive(Serialize, Deserialize, Debug, Hash, Eq, PartialEq, Clone)]
pub(crate) struct PubId {
    pub(crate) origin_id: Id,
    pub(crate) seqn: u32,
}

impl Copy for PubId {}
