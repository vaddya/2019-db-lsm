package ru.mail.polis.vaddya;

public final class TimeUtils {
    private static volatile int COUNTER = 0;
    private static volatile long LAST_MILLIS = 0L;

    private TimeUtils() {
    }

    /**
     * Emulate current time in nanoseconds using System.currentTimeMillis and atomic counter
     */
    public static synchronized long currentTimeNanos() {
        final var millis = System.currentTimeMillis();
        if (LAST_MILLIS != millis) {
            LAST_MILLIS = millis;
            COUNTER = 0;
        }
        return LAST_MILLIS * 1_000_000 + COUNTER++;
    }
}
