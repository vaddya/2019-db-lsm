package ru.mail.polis.vaddya;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;

public final class SSTableIndex implements Iterable<SSTableIndexEntry> {
    private final NavigableMap<ByteBuffer, SSTableIndexEntry> entries;

    /**
     * Create sorted strings table index using timestamp,
     * file containing index and file containing data.
     *
     * @param ts        timestamp when data was saved
     * @param indexPath path to file containing index
     * @param dataPath  path to file containing data
     * @return an sorted strings table instance
     * @throws IOException if cannot read from files
     */
    @NotNull
    public static SSTableIndex from(@NotNull final LocalDateTime ts,
                                    @NotNull final Path indexPath,
                                    @NotNull final Path dataPath) throws IOException {

        final var bytes = Files.readAllBytes(indexPath);
        final var entries = new TreeMap<ByteBuffer, SSTableIndexEntry>();
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        while (buffer.hasRemaining()) {
            final var keySize = buffer.getInt();
            final var key = ByteBuffer.allocate(keySize);
            buffer.get(key.array());
            final var offset = buffer.getInt();
            final var size = buffer.getInt();
            final var deleted = buffer.get() == Byte.MAX_VALUE;

            final var dataPointer = FileDataPointer.to(dataPath, offset, size);
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
