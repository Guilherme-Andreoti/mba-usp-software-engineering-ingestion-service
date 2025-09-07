package mba.usp.distributed.architecture.ingestion_service.config;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MqttConfig {

    private static final String BROKER_URL = "tcp://host.docker.internal:1883"; // ou IP do Mosquitto
    private static final String CLIENT_ID = "springboot-client";

    public MqttConfig() {
        System.out.println("Iniciando MqttConfig...");
    }

    @Bean
    public MqttClient mqttClient() throws MqttException, MqttException {
        MqttClient client = new MqttClient(BROKER_URL, CLIENT_ID, new MemoryPersistence());
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(false);
        options.setAutomaticReconnect(true);
        options.setConnectionTimeout(2);

        client.connect(options);
        return client;
    }
}
