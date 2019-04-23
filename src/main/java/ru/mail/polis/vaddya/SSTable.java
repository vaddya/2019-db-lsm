package ru.mail.polis.vaddya;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;

public final class SSTable implements Iterable<TableEntry> {
    private final NavigableMap<ByteBuffer, TableEntry> entries;

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
    public static SSTable from(@NotNull final LocalDateTime ts,
                               @NotNull final FileChannel indexPath,
                               @NotNull final FileChannel dataPath) throws IOException {
        final var entries = new TreeMap<ByteBuffer, TableEntry>();
        final var buffer = ByteBuffer.allocate((int) indexPath.size());
        indexPath.read(buffer);
        buffer.flip();
        while (buffer.hasRemaining()) {
            final var keySize = buffer.getInt();
            final var key = ByteBuffer.allocate(keySize);
            buffer.get(key.array());
            final var offset = buffer.getInt();
            final var size = buffer.getInt();
            final var hasTombstone = buffer.get() == Byte.MAX_VALUE;

            final var value = dataPath.map(READ_ONLY, offset, size);
            entries.put(key, TableEntry.from(key, value, hasTombstone, ts));
        }
        return new SSTable(entries);
    }

    private SSTable(@NotNull final NavigableMap<ByteBuffer, TableEntry> entries) {
        this.entries = entries;
    }

    @NotNull
    public Iterator<TableEntry> iteratorFrom(@NotNull final ByteBuffer from) {
        return entries.tailMap(from).values().iterator();
    }

    @NotNull
    @Override
    public Iterator<TableEntry> iterator() {
        return entries.values().iterator();
    }
}
