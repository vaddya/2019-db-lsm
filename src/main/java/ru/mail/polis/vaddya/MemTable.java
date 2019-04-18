package ru.mail.polis.vaddya;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;

import static java.nio.file.StandardOpenOption.APPEND;
import static ru.mail.polis.vaddya.ByteUtils.readBytesFromByteBuffer;
import static ru.mail.polis.vaddya.ByteUtils.writeIntToByteArray;

public class MemTable implements Iterable<MemTableEntry> {
    private final NavigableMap<ByteBuffer, MemTableEntry> table = new TreeMap<>();
    private int currentSize = 0;

    public int getCurrentSize() {
        return currentSize;
    }

    public void upsert(ByteBuffer key, Record value) {
        table.put(key, MemTableEntry.upsert(value));
        currentSize += value.getValue().remaining();
    }

    public void remove(ByteBuffer key) {
        MemTableEntry.delete(key);
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

    public void dumpTo(@NotNull final File indexFile,
                       @NotNull final File dataFile) throws IOException {
        var offset = 0;
        for (var entry : table.values()) {
            final var valueBytes = dataToBytes(entry);
            Files.write(dataFile.toPath(), valueBytes, APPEND);

            final var indexBytes = indexToBytes(entry, offset);
            Files.write(indexFile.toPath(), indexBytes, APPEND);

            offset += valueBytes.length;
        }
        table.clear();
        currentSize = 0;
    }

    @NotNull
    private byte[] dataToBytes(@NotNull final MemTableEntry entry) {
        return readBytesFromByteBuffer(entry.getValue());
    }

    @NotNull
    private byte[] indexToBytes(@NotNull final MemTableEntry entry,
                                final int dataOffset) {
        final var keySize = entry.getKey().remaining();
        final var valueSize = entry.getValue().remaining();
        final var bytes = new byte[4 + keySize + 4 + 4 + 1];

        writeIntToByteArray(bytes, keySize, 0);
        entry.getKey().get(bytes, 4, keySize);
        writeIntToByteArray(bytes, dataOffset, 4 + keySize);
        writeIntToByteArray(bytes, valueSize, 4 + keySize + 4);
        bytes[bytes.length - 1] = (byte) (entry.isDeleted() ? 1 : 0);

        return bytes;
    }
}
