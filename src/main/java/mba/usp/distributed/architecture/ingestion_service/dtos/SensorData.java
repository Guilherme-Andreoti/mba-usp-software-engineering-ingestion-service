package mba.usp.distributed.architecture.ingestion_service.dtos;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.LocalDateTime;


public class SensorData {

    private String id;
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    @JsonProperty("date_time")
    private LocalDateTime dateTime;
    private double proximity;
    private double humidity;
    private double pressure;
    private double light;
    private double oxidised;
    private double reduced;
    private double nh3;
    private double temperature;
    private SoundData sound;
    private String topic;
    private long startProcessingTimestamp;

    public SensorData() {

    }

    public SensorData(LocalDateTime dateTime, double proximity, double humidity, double pressure, double light,
                      double oxidised, double reduced, double nh3, double temperature, SoundData sound, String topic) {
        this.dateTime = dateTime;
        this.proximity = proximity;
        this.humidity = humidity;
        this.pressure = pressure;
        this.light = light;
        this.oxidised = oxidised;
        this.reduced = reduced;
        this.nh3 = nh3;
        this.temperature = temperature;
        this.sound = sound;
        this.topic = topic;
    }

    public LocalDateTime getDateTime() {
        return dateTime;
    }

    public double getProximity() {
        return proximity;
    }

    public double getHumidity() {
        return humidity;
    }

    public double getPressure() {
        return pressure;
    }

    public double getLight() {
        return light;
    }

    public double getOxidised() {
        return oxidised;
    }

    public double getReduced() {
        return reduced;
    }

    public double getNh3() {
        return nh3;
    }

    public double getTemperature() {
        return temperature;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setStartProcessingTimestamp(long startProcessingTimestamp) {
        this.startProcessingTimestamp = startProcessingTimestamp;
    }

    public String getTopic() {return topic;}


    public SoundData getSound() {
        return sound;
    }

    public long getStartProcessingTimestamp() {
        return startProcessingTimestamp;
    }
}