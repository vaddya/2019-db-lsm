package ru.mail.polis.vaddya;

public final class TimeUtils {
    private static int COUNTER;
    private static long LAST_MILLIS;

    private TimeUtils() {
    }

    /**
     * Emulate current time in nanoseconds using System.currentTimeMillis and atomic counter
     */
    public static long currentTimeNanos() {
        synchronized (TimeUtils.class) {
            final var millis = System.currentTimeMillis();
            if (LAST_MILLIS != millis) {
                LAST_MILLIS = millis;
                COUNTER = 0;
            }
            return LAST_MILLIS * 1_000_000 + COUNTER++;
        }
    }
}
