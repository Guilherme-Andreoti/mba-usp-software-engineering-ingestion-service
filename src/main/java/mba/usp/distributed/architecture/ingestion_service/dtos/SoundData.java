package mba.usp.distributed.architecture.ingestion_service.dtos;

public class SoundData {
    private double high;
    private double mid;
    private double low;
    private double amplitude;

    public SoundData(){}

    public SoundData(double high, double mid, double low, double amplitude) {
        this.high = high;
        this.mid = mid;
        this.low = low;
        this.amplitude = amplitude;
    }

    public double getHigh() {
        return high;
    }

    public double getMid() {
        return mid;
    }

    public double getLow() {
        return low;
    }

    public double getAmplitude() {
        return amplitude;
    }
}
