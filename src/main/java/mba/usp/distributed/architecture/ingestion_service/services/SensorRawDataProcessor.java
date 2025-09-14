package mba.usp.distributed.architecture.ingestion_service.services;


import com.fasterxml.jackson.databind.ObjectMapper;
import mba.usp.distributed.architecture.ingestion_service.messaging.SensorDataPublisher;
import mba.usp.distributed.architecture.ingestion_service.dtos.SensorData;
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

    public void handleMessage(String topic, MqttMessage message) {
        long currentTimestamp = System.currentTimeMillis();
        try {
            String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
            if (payload.isBlank()) {
                System.out.println("⚠️ Mensagem vazia recebida no tópico: " + topic);
                return;
            }

            SensorData sensorData = objectMapper.readValue(payload, SensorData.class);
            sensorData.setTopic(topic);
            sensorData.setStartProcessingTimestamp(currentTimestamp);

            sensorDataPublisher.publish(sensorData);

        } catch (Exception e) {
            System.err.println("❌ Erro ao processar mensagem MQTT: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
