package mba.usp.distributed.architecture.ingestion_service.messaging;


import mba.usp.distributed.architecture.ingestion_service.config.RabbitMQConfig;
import mba.usp.distributed.architecture.ingestion_service.dtos.SensorData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

@Component
public class SensorDataPublisher {

    private final RabbitTemplate rabbitTemplate;

    public SensorDataPublisher(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    public void publish(SensorData sensorData) {
        rabbitTemplate.convertAndSend(
                RabbitMQConfig.EXCHANGE_NAME,
                RabbitMQConfig.ROUTING_KEY,
                sensorData
        );
        System.out.println("📤 Dados publicados no RabbitMQ: " + sensorData);
    }
}