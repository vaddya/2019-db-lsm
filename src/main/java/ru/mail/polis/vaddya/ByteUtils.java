package ru.mail.polis.vaddya;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public final class ByteUtils {
    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    private ByteUtils() {
    }

    @NotNull
    public static ByteBuffer emptyBuffer() {
        return EMPTY_BUFFER;
    }
}
