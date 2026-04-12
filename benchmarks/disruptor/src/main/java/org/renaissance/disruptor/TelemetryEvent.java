package org.renaissance.disruptor;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public final class TelemetryEvent {

    public PartialEventType type;
    public boolean isReady;
    public boolean isAnomaly;

    public static final int DATA_BYTE_SIZE = 112;
    public long observationId;
    public long dataSourceId;
    public final double[] torques = new double[6];
    public final double[] temperatures = new double[6];

    public int writeDataTo(ByteBuffer dst, int offset) {
        int pos = dst.position();
        dst.order(ByteOrder.nativeOrder());
        dst.putLong(offset, observationId);
        dst.putLong(offset + 8, dataSourceId);
        for (int i = 0; i < 6; i++) {
            dst.putDouble(offset + 16 + i * 8, torques[i]);
        }
        for (int i = 0; i < 6; i++) {
            dst.putDouble(offset + 64 + i * 8, temperatures[i]);
        }
        dst.position(pos);
        return DATA_BYTE_SIZE;
    }
}
