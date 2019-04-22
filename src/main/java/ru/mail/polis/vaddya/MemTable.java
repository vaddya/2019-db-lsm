package ru.mail.polis.vaddya;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;

public class MemTable implements Iterable<MemTableEntry> {
    private final NavigableMap<ByteBuffer, MemTableEntry> table = new TreeMap<>();
    private int currentSize;

    public int getCurrentSize() {
        return currentSize;
    }

    public void upsert(@NotNull final ByteBuffer key,
                       @NotNull final Record value) {
        table.put(key, MemTableEntry.upsert(value));
        currentSize += value.getValue().remaining();
    }

    public void remove(@NotNull final ByteBuffer key) {
        table.put(key, MemTableEntry.delete(key));
    }

    @NotNull
    @Override
    public Iterator<MemTableEntry> iterator() {
        return table.values().iterator();
    }

    @NotNull
    public Iterator<MemTableEntry> iteratorFrom(@NotNull final ByteBuffer from) {
        return table.tailMap(from).values().iterator();
    }

    /**
     * Dump current mem table to specified files.
     *
     * @param indexFile file to store index
     * @param dataFile  file to store data
     * @throws IOException if cannot write data
     */
    public void dumpTo(@NotNull final FileChannel indexFile,
                       @NotNull final FileChannel dataFile) throws IOException {
        var offset = 0;
        for (final var entry : table.values()) {
            dataFile.write(dataToBytes(entry));
            indexFile.write(indexToBytes(entry, offset));
            offset += entry.getValue().remaining();
        }
        table.clear();
        currentSize = 0;
    }

    private ByteBuffer dataToBytes(@NotNull final MemTableEntry entry) {
        return entry.getValue(); // TODO: maybe need also to save key to data file
    }

    /**
     * Create and fill ByteBuffer from MemTable entry.
     * <p>ByteBuffer contains:
     * <ul>
     * <li> Size of the key (4 bytes)
     * <li> Key of the entry (N bytes)
     * <li> Offset inside data file (4 bytes)
     * <li> Size of the value inside data file (4 bytes)
     * <li> Tombstone - a marker indicates that value was deleted (1 byte)
     * </ul></p>
     */
    @NotNull
    private ByteBuffer indexToBytes(@NotNull final MemTableEntry entry,
                                    final int dataOffset) {
        final var keySize = entry.getKey().remaining();
        final var valueSize = entry.getValue().remaining();

        return ByteBuffer.allocate(keySize + Integer.BYTES * 3 + Byte.BYTES)
                .putInt(keySize)
                .put(entry.getKey().duplicate())
                .putInt(dataOffset)
                .putInt(valueSize)
                .put(entry.isDeleted() ? Byte.MAX_VALUE : Byte.MIN_VALUE)
                .flip();
    }
}
