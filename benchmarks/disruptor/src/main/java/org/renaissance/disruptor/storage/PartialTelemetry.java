package org.renaissance.disruptor.storage;

import org.renaissance.disruptor.util.TelemetryEvent;

import java.nio.ByteBuffer;

public final class PartialTelemetry {
    public static final int SIZE_IN_BYTES = 128; // 8+8+4+48+48 = 116 bytes -> 128 aligned to cache line size

    private long observationId = -1;
    private long dataSourceId;
    private final double[] torques = new double[6];
    private final double[] temperatures = new double[6];
    private int partsMask = 0;

    public boolean isComplete() { return partsMask == 7; }
    public void reset() { partsMask = 0; observationId = 0; }

    void readFrom(ByteBuffer buffer, int offset) {
        this.observationId = buffer.getLong(offset);
        this.dataSourceId = buffer.getLong(offset + 8);
        this.partsMask = buffer.getInt(offset + 16);
        for (int i = 0; i < 6; i++) {
            this.torques[i] = buffer.getDouble(offset + 24 + i * 8);
        }
        for (int i = 0; i < 6; i++) {
            this.temperatures[i] = buffer.getDouble(offset + 72 + i * 8);
        }
    }

    void writeTo(ByteBuffer buffer, int offset) {
        buffer.putLong(offset, this.observationId);
        buffer.putLong(offset + 8, this.dataSourceId);
        buffer.putInt(offset + 16, this.partsMask);
        for (int i = 0; i < 6; i++) {
            buffer.putDouble(offset + 24 + i * 8, this.torques[i]);
        }
        for (int i = 0; i < 6; i++) {
            buffer.putDouble(offset + 72 + i * 8, this.temperatures[i]);
        }
    }

    public void updateFrom(TelemetryEvent event) {
        switch (event.type) {
            case DATA_SOURCE_ID:
                this.dataSourceId = event.dataSourceId;
                this.partsMask |= 1;
                break;
            case TORQUE:
                System.arraycopy(event.torques, 0, this.torques, 0, 6);
                this.partsMask |= 2;
                break;
            case TEMPERATURE:
                System.arraycopy(event.temperatures, 0, this.temperatures, 0, 6);
                this.partsMask |= 4;
                break;
        }
    }

    public void copyTo(TelemetryEvent event) {
        event.dataSourceId = this.dataSourceId;
        System.arraycopy(this.torques, 0, event.torques, 0, 6);
        System.arraycopy(this.temperatures, 0, event.temperatures, 0, 6);
    }

    public int getPartsMask() {
        return partsMask;
    }

    public void setPartsMask(int partsMask) {
        this.partsMask = partsMask;
    }

    public double[] getTemperatures() {
        return temperatures;
    }

    public double[] getTorques() {
        return torques;
    }

    public long getDataSourceId() {
        return dataSourceId;
    }

    public void setDataSourceId(long dataSourceId) {
        this.dataSourceId = dataSourceId;
    }

    public long getObservationId() {
        return observationId;
    }

    public void setObservationId(long observationId) {
        this.observationId = observationId;
    }
}
