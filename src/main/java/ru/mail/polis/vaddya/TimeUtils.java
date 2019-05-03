package ru.mail.polis.vaddya;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class TimeUtils {
    private static final AtomicInteger COUNTER = new AtomicInteger(0);
    private static final AtomicLong LAST_MILLIS = new AtomicLong(0);

    private TimeUtils() {
    }

    /**
     * Emulate current time in nanoseconds using System.currentTimeMillis and atomic counter
     */
    public static long currentTimeNanos() {
        long millis = System.currentTimeMillis(); // 100
        if (LAST_MILLIS.get() != millis) {
            LAST_MILLIS.set(millis);
            COUNTER.set(0);
        }
        return LAST_MILLIS.get() * 1_000_000 + COUNTER.getAndIncrement();
    }
}
