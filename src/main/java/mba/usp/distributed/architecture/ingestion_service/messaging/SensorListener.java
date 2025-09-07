package mba.usp.distributed.architecture.ingestion_service.messaging;

import io.micrometer.core.annotation.Timed;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import mba.usp.distributed.architecture.ingestion_service.service.SensorRawDataProcessor;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.springframework.stereotype.Service;

@Service
public class SensorListener {
    private final MqttClient mqttClient;
    private final Counter messageCounter;
    private final SensorRawDataProcessor sensorRawDataProcessor;

    public SensorListener(MqttClient mqttClient, MeterRegistry registry, SensorRawDataProcessor sensorRawDataProcessor) {
        this.mqttClient = mqttClient;
        this.messageCounter = Counter.builder("mqtt_messages_received")
                .description("Number of MQTT messages received")
                .register(registry);

        this.sensorRawDataProcessor = sensorRawDataProcessor;
    }

    @PostConstruct
    public void subscribe() throws Exception {
        mqttClient.subscribe("#", (topic, message) -> {
            messageCounter.increment();
            sensorRawDataProcessor.handleMessage(topic,message);

        });
    }
}
