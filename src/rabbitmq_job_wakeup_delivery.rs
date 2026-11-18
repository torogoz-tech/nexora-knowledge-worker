use lapin::{message::Delivery, options::BasicAckOptions, options::BasicNackOptions};
use nexora_platform::PlatformError;

pub(crate) struct RabbitMqJobWakeupDelivery {
    delivery: Delivery,
}

impl RabbitMqJobWakeupDelivery {
    pub(crate) fn new(delivery: Delivery) -> Self {
        Self { delivery }
    }

    pub(crate) async fn acknowledge(self) -> Result<(), PlatformError> {
        self.delivery
            .ack(BasicAckOptions::default())
            .await
            .map(|_| ())
            .map_err(|_| PlatformError::Unavailable)
    }

    pub(crate) async fn requeue(self) -> Result<(), PlatformError> {
        self.delivery
            .nack(BasicNackOptions {
                requeue: true,
                ..Default::default()
            })
            .await
            .map(|_| ())
            .map_err(|_| PlatformError::Unavailable)
    }
}
