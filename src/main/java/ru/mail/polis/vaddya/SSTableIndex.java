package ru.mail.polis.vaddya;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;

import static ru.mail.polis.vaddya.ByteUtils.readIntFromByteArray;

public class SSTableIndex implements Iterable<SSTableIndexEntry> {
    private final NavigableMap<ByteBuffer, SSTableIndexEntry> entries;

    @NotNull
    public static SSTableIndex from(@NotNull final LocalDateTime ts,
                                    @NotNull final File indexFile,
                                    @NotNull final File dataFile) throws IOException {
        final byte[] bytes = Files.readAllBytes(indexFile.toPath());
        final NavigableMap<ByteBuffer, SSTableIndexEntry> entries = new TreeMap<>();
        var i = 0;
        while (i < bytes.length) {
            final var keySize = readIntFromByteArray(bytes, i);
            i += 4;

            final var keyBytes = new byte[keySize];
            System.arraycopy(bytes, i, keyBytes, 0, keySize);
            ByteBuffer key = ByteBuffer.wrap(keyBytes);
            i += keySize;
            final var offset = readIntFromByteArray(bytes, i);
            i += 4;

            final var size = readIntFromByteArray(bytes, i);
            i += 4;

            final boolean deleted = bytes[i] == 1;
            i += 1;

            final var dataPointer = new FileDataPointer(dataFile, offset, size);
            entries.put(key, SSTableIndexEntry.from(key, ts, dataPointer, deleted));
        }

        return new SSTableIndex(entries);
    }

    private SSTableIndex(@NotNull final NavigableMap<ByteBuffer, SSTableIndexEntry> entries) {
        this.entries = entries;
    }

    @NotNull
    public Iterator<SSTableIndexEntry> iteratorFrom(@NotNull final ByteBuffer from) {
        return entries.tailMap(from).values().iterator();
    }

    @NotNull
    @Override
    public Iterator<SSTableIndexEntry> iterator() {
        return entries.values().iterator();
    }
}
