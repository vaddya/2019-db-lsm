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

    /**
     * Read bytes from buffer to byte array.
     *
     * @param byteBuffer buffer
     * @return read bytes
     */
    @NotNull
    public static byte[] readBytesFromByteBuffer(@NotNull final ByteBuffer byteBuffer) {
        final var valueSize = byteBuffer.remaining();
        final var bytes = new byte[valueSize];
        byteBuffer.get(bytes);
        return bytes;
    }

    /**
     * Write integer to byte array with specified offset.
     *
     * @param dest   a byte array to write
     * @param value  integer to be written
     * @param offset offset inside a byte array
     */
    public static void writeIntToByteArray(@NotNull final byte[] dest,
                                           final int value,
                                           final int offset) {
        final var bytes = Ints.toByteArray(value);
        System.arraycopy(bytes, 0, dest, offset, bytes.length);
    }

    /**
     * Read integer from byte array with specified offset.
     *
     * @param src    a byte array to read
     * @param offset offset inside a byte array
     * @return read value
     */
    public static int readIntFromByteArray(@NotNull final byte[] src,
                                           final int offset) {
        return Ints.fromBytes(src[offset], src[offset + 1], src[offset + 2], src[offset + 3]);
    }

}
