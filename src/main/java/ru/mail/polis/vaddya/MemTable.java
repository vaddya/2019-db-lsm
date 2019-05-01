package ru.mail.polis.vaddya;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;

public class MemTable implements Table {
    private final NavigableMap<ByteBuffer, TableEntry> table = new TreeMap<>();
    private int currentSize;

    @Override
    @NotNull
    public Iterator<TableEntry> iterator(@NotNull final ByteBuffer from) {
        return table.tailMap(from).values().iterator();
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key,
                       @NotNull final ByteBuffer value) {
        table.put(key, TableEntry.upsert(key, value));
        currentSize += Integer.BYTES + key.remaining() + Long.BYTES + Integer.BYTES + value.remaining();
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        table.put(key, TableEntry.delete(key));
        currentSize += Integer.BYTES + key.remaining() + Long.BYTES;
    }

    public int getCurrentSize() {
        return currentSize;
    }

    /**
     * Dump current mem table to specified files.
     *
     * @param channel file to store index
     * @throws IOException if cannot write data
     */
    public void flushTo(@NotNull final FileChannel channel) throws IOException {
        final var offsetsBuffer = ByteBuffer.allocate(table.size() * Integer.BYTES);
        var offset = 0;
        for (final var entry : table.values()) {
            offsetsBuffer.putInt(offset);
            final var buffer = entryToByteBuffer(entry);
            offset += buffer.remaining();
            channel.write(buffer);
        }
        channel.write(offsetsBuffer.flip());

        final var sizeBuffer = ByteBuffer.allocate(Integer.BYTES)
                .putInt(table.size())
                .flip();
        channel.write(sizeBuffer);

        table.clear();
        currentSize = 0;
    }

    /**
     * Create and fill ByteBuffer from MemTable entry.
     *
     * <p>ByteBuffer contains:
     * <ul>
     * <li> Size of the key (4 bytes)
     * <li> Key of the entry (N bytes)
     * <li> Timestamp, if negative then it is a tombstone and nether value size nor value itself are present
     * <li> Size of the value (4 bytes)
     * <li> Value of the entry (M bytes)
     * </ul>
     */
    @NotNull
    private ByteBuffer entryToByteBuffer(@NotNull final TableEntry entry) {
        final var keySize = entry.getKey().remaining();
        if (entry.hasTombstone()) {
            return ByteBuffer.allocate(Integer.BYTES + keySize + Long.BYTES)
                    .putInt(keySize)
                    .put(entry.getKey())
                    .putLong(-entry.ts())
                    .flip();
        }

        final var valueSize = entry.getValue().remaining();
        return ByteBuffer.allocate(Integer.BYTES + keySize + Long.BYTES + Integer.BYTES + valueSize)
                .putInt(keySize)
                .put(entry.getKey())
                .putLong(entry.ts())
                .putInt(valueSize)
                .put(entry.getValue())
                .flip();
    }
}
