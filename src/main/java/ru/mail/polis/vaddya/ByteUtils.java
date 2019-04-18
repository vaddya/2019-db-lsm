package ru.mail.polis.vaddya;

import com.google.common.primitives.Ints;
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

    @NotNull
    public static byte[] readBytesFromByteBuffer(@NotNull final ByteBuffer byteBuffer) {
        final var valueSize = byteBuffer.remaining();
        final var bytes = new byte[valueSize];
        byteBuffer.get(bytes);
        return bytes;
    }

    public static void writeIntToByteArray(@NotNull final byte[] dest,
                                           final int value,
                                           final int position) {
        final var bytes = Ints.toByteArray(value);
        System.arraycopy(bytes, 0, dest, position, bytes.length);
    }

    public static int readIntFromByteArray(@NotNull final byte[] src,
                                           final int position) {
        return Ints.fromBytes(src[position], src[position + 1], src[position + 2], src[position + 3]);
    }

}
