use lapin::{
    Channel, ExchangeKind,
    options::{ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions},
    types::{AMQPValue, FieldTable},
};
use nexora_platform::{JobKind, PlatformError};

const JOBS_EXCHANGE: &str = "nexora.jobs";
const DEAD_LETTER_EXCHANGE: &str = "nexora.jobs.dead";
const DEAD_LETTER_QUEUE: &str = "nexora.jobs.dead";

pub(crate) struct RabbitMqJobWakeupTopology;

impl RabbitMqJobWakeupTopology {
    pub(crate) async fn declare(channel: &Channel) -> Result<(), PlatformError> {
        channel
            .exchange_declare(
                JOBS_EXCHANGE.into(),
                ExchangeKind::Topic,
                ExchangeDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|_| PlatformError::Unavailable)?;
        channel
            .exchange_declare(
                DEAD_LETTER_EXCHANGE.into(),
                ExchangeKind::Fanout,
                ExchangeDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|_| PlatformError::Unavailable)?;
        channel
            .queue_declare(
                DEAD_LETTER_QUEUE.into(),
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|_| PlatformError::Unavailable)?;
        channel
            .queue_bind(
                DEAD_LETTER_QUEUE.into(),
                DEAD_LETTER_EXCHANGE.into(),
                "".into(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|_| PlatformError::Unavailable)?;

        let mut arguments = FieldTable::default();
        arguments.insert(
            "x-dead-letter-exchange".into(),
            AMQPValue::LongString(DEAD_LETTER_EXCHANGE.into()),
        );
        let queue = JobKind::KnowledgeSynthesis.queue_name();
        channel
            .queue_declare(
                queue.into(),
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                arguments,
            )
            .await
            .map_err(|_| PlatformError::Unavailable)?;
        channel
            .queue_bind(
                queue.into(),
                JOBS_EXCHANGE.into(),
                JobKind::KnowledgeSynthesis.routing_key().into(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|_| PlatformError::Unavailable)?;
        Ok(())
    }
}
