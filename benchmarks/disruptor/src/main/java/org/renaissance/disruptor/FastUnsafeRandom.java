package org.renaissance.disruptor;

import java.util.Random;

/**
 * Non-thread-safe version of {@link java.util.Random}.
 */
public class FastUnsafeRandom extends Random {

    private static final long multiplier = 0x5DEECE66DL;
    private static final long addend = 0xBL;
    private static final long mask = (1L << 48) - 1;

    private long currentSeed;

    public FastUnsafeRandom(long seed) {
        super();
        this.currentSeed = seed;
    }

    public FastUnsafeRandom() {
        super();
    }

    @Override
    public void setSeed(long seed) {
        this.currentSeed = (seed ^ multiplier) & mask;
    }

    @Override
    protected int next(int bits) {
        currentSeed = (currentSeed * multiplier + addend) & mask;
        return (int) (currentSeed >>> (48 - bits));
    }
}
