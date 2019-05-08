package ru.mail.polis.vaddya;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

final class ByteBufferUtils {
    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    private ByteBufferUtils() {
    }

    @NotNull
    public static ByteBuffer emptyBuffer() {
        return EMPTY_BUFFER;
    }
}
