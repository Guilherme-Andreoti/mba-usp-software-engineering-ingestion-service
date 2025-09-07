package mba.usp.distributed.architecture.ingestion_service.service;


import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.annotation.Timed;
import mba.usp.distributed.architecture.ingestion_service.messaging.SensorDataPublisher;
import mba.usp.distributed.architecture.ingestion_service.model.SensorData;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
public class SensorRawDataProcessor {

    private final ObjectMapper objectMapper;
    private final SensorDataPublisher sensorDataPublisher;

    public SensorRawDataProcessor(ObjectMapper objectMapper, SensorDataPublisher sensorDataPublisher) {
        this.objectMapper = objectMapper;
        this.sensorDataPublisher = sensorDataPublisher;
    }

    @Timed(
            value = "iot.processing",
            description = "Tempo de processamento",
            extraTags = {"service", "ingestion-service"}
    )
    public void handleMessage(String topic, MqttMessage message) {
        try {
            String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
            if (payload.isBlank()) {
                System.out.println("⚠️ Mensagem vazia recebida no tópico: " + topic);
                return;
            }

            SensorData sensorData = objectMapper.readValue(payload, SensorData.class);
            sensorData.setTopic(topic);

            sensorDataPublisher.publish(sensorData);

        } catch (Exception e) {
            System.err.println("❌ Erro ao processar mensagem MQTT: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
