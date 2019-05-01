package ru.mail.polis.vaddya;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;

import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;

public final class SSTable implements Table {
    private final int entriesCount;
    private final IntBuffer offsets;
    private final ByteBuffer entries;

    /**
     * Create sorted strings table index using timestamp,
     * file containing index and file containing data.
     *
     * @param channel channel to read index from
     * @return an sorted strings table instance
     * @throws IOException if cannot read from files
     */
    @NotNull
    public static SSTable from(@NotNull final FileChannel channel) throws IOException {
        final var mapped = channel.map(READ_ONLY, 0, channel.size()).order(BIG_ENDIAN);

        final var entriesCount = mapped.getInt(mapped.limit() - Integer.BYTES);
        final var offsetsBuffer = mapped.duplicate()
                .position(mapped.limit() - Integer.BYTES - Integer.BYTES * entriesCount)
                .limit(mapped.limit() - Integer.BYTES)
                .slice()
                .asReadOnlyBuffer();
        final var offsets = offsetsBuffer.slice().asIntBuffer();
        final var entries = mapped.duplicate()
                .position(0)
                .limit(mapped.limit() - Integer.BYTES - Integer.BYTES * entriesCount)
                .slice()
                .asReadOnlyBuffer();

        return new SSTable(entriesCount, offsets, entries);
    }

    private SSTable(final int entriesCount,
                    @NotNull final IntBuffer offsets,
                    @NotNull final ByteBuffer entries) {
        this.entriesCount = entriesCount;
        this.entries = entries;
        this.offsets = offsets;
    }

    @Override
    @NotNull
    public Iterator<TableEntry> iterator(@NotNull final ByteBuffer from) {
        return new Iterator<>() {
            private int position = position(from);

            @Override
            public boolean hasNext() {
                return position < entriesCount;
            }

            @Override
            public TableEntry next() {
                return entryAt(position++);
            }
        };
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key,
                       @NotNull final ByteBuffer value) {
        throw new UnsupportedOperationException("SSTable is immutable");
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        throw new UnsupportedOperationException("SSTable is immutable");
    }

    private int position(@NotNull final ByteBuffer key) {
        var left = 0;
        var right = entriesCount - 1;
        while (left <= right) {
            final var mid = left + (right - left) / 2;
            final var cmp = keyAt(mid).compareTo(key);
            if (cmp < 0) {
                left = mid + 1;
            } else if (cmp > 0) {
                right = mid - 1;
            } else {
                return mid;
            }
        }
        return left;
    }

    @NotNull
    private ByteBuffer keyAt(final int position) {
        final var offset = offsets.get(position);
        final var keySize = entries.getInt(offset);
        return entries.duplicate()
                .position(offset + Integer.BYTES)
                .limit(offset + Integer.BYTES + keySize)
                .slice();
    }

    @NotNull
    private TableEntry entryAt(final int position) {
        var offset = offsets.get(position);

        final var keySize = entries.getInt(offset);
        offset += Integer.BYTES;

        final var key = entries.duplicate()
                .position(offset)
                .limit(offset + keySize)
                .slice();
        offset += keySize;

        final var ts = entries.position(offset).getLong();
        offset += Long.BYTES;
        if (ts < 0) {
            return TableEntry.from(key, null, true, -ts);
        }

        final var valueSize = entries.getInt(offset);
        offset += Integer.BYTES;

        final var value = entries.duplicate()
                .position(offset)
                .limit(offset + valueSize)
                .slice();

        return TableEntry.from(key, value, false, ts);
    }
}
