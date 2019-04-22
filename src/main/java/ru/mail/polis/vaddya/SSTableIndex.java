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

    /**
     * Create sorted strings table index using timestamp,
     * file containing index and file containing data.
     *
     * @param ts        timestamp when data was saved
     * @param indexFile file containing index
     * @param dataFile  file containing data
     * @return an sorted strings table instance
     * @throws IOException if cannot read from files
     */
    @NotNull
    public static SSTableIndex from(@NotNull final LocalDateTime ts,
                                    @NotNull final File indexFile,
                                    @NotNull final File dataFile) throws IOException {
        final var bytes = Files.readAllBytes(indexFile.toPath());
        final var entries = new TreeMap<ByteBuffer, SSTableIndexEntry>();
        var idx = 0;
        while (idx < bytes.length) {
            final var keySize = readIntFromByteArray(bytes, idx);
            idx += Integer.BYTES;

            final var key = ByteBuffer.allocate(keySize)
                    .put(bytes, idx, keySize)
                    .position(0);
            idx += keySize;

            final var offset = readIntFromByteArray(bytes, idx);
            idx += 4;

            final var size = readIntFromByteArray(bytes, idx);
            idx += 4;

            final boolean deleted = bytes[idx] == 1;
            idx += 1;

            final var dataPointer = FileDataPointer.to(dataFile, offset, size);
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
