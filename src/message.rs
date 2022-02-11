use crate::federator::NEIGHBOURS_QOS;
use crate::topic::*;
use core::fmt;
use paho_mqtt as mqtt;
use serde::{Deserialize, Serialize};

pub(crate) enum Message {
    FederatedPub(mqtt::Message),
    CoreAnn(CoreAnn),
    MeshMembAnn(MeshMembAnn),
    Beacon,
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct CoreAnn {
    pub core_id: u32,
    pub seq_number: u32,
    pub dist_to_core: u32,
    pub sender_id: u32,
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct MeshMembAnn {
    pub core_id: u32,
    pub seq_number: u32,
    pub sender_id: u32,
}

pub(crate) fn deserialize(mqtt_msg: mqtt::Message) -> Result<(String, Message), MessageError> {
    let topic = mqtt_msg.topic();

    if let Some(topic) = topic.strip_prefix(FEDERATED_TOPICS_LEVEL) {
        assert_not_empty(topic)?;
        Ok((topic.to_owned(), Message::FederatedPub(mqtt_msg)))
    } else if let Some(topic) = topic.strip_prefix(CORE_ANN_TOPIC_LEVEL) {
        assert_not_empty(topic)?;
        match bincode::deserialize(mqtt_msg.payload()) {
            Ok(core_ann) => Ok((topic.to_owned(), Message::CoreAnn(core_ann))),
            Err(err) => Err(MessageError::DeserializationError(err)),
        }
    } else if let Some(topic) = topic.strip_prefix(MEMB_ANN_TOPIC_LEVEL) {
        assert_not_empty(topic)?;
        match bincode::deserialize(mqtt_msg.payload()) {
            Ok(memb_ann) => Ok((topic.to_owned(), Message::MeshMembAnn(memb_ann))),
            Err(err) => Err(MessageError::DeserializationError(err)),
        }
    } else if let Some(topic) = topic.strip_prefix(BEACON_TOPIC_LEVEL) {
        assert_not_empty(topic)?;
        Ok((topic.to_owned(), Message::Beacon))
    } else {
        panic!("received a message from a topic it was not supposed to be subscribed to");
    }
}

impl CoreAnn {
    pub(crate) fn serialize(&self, fed_topic: &str) -> mqtt::Message {
        let topic = format!("{CORE_ANN_TOPIC_LEVEL}{fed_topic}");
        let payload = bincode::serialize(self).unwrap();

        mqtt::Message::new(topic, payload, NEIGHBOURS_QOS)
    }
}

impl MeshMembAnn {
    pub(crate) fn serialize(&self, fed_topic: &str) -> mqtt::Message {
        let topic = format!("{MEMB_ANN_TOPIC_LEVEL}{fed_topic}");
        let payload = bincode::serialize(self).unwrap();

        mqtt::Message::new(topic, payload, NEIGHBOURS_QOS)
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

impl Message {
    pub(crate) fn is_core_ann(&self) -> bool {
        match self {
            Message::CoreAnn(_) => true,
            _ => false,
        }
    }

    pub(crate) fn is_beacon(&self) -> bool {
        match self {
            Message::Beacon => true,
            _ => false,
        }
    }
}
