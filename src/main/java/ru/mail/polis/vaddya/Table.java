package ru.mail.polis.vaddya;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;

import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;

interface Table {
    /**
     * Get iterator over the table entries starting from the given key.
     */
    @NotNull
    Iterator<TableEntry> iterator(@NotNull ByteBuffer from);

    /**
     * Get current size of the table entries in bytes.
     */
    int currentSize();

    /**
     * Insert a value into the table using the given key.
     *
     * @throws UnsupportedOperationException if table is immutable
     */
    default void upsert(
            @NotNull final ByteBuffer key,
            @NotNull final ByteBuffer value) {
        throw new UnsupportedOperationException("Table is immutable");
    }

    /**
     * Remove a value from the table using the given key.
     *
     * @throws UnsupportedOperationException if table is immutable
     */
    default void remove(@NotNull final ByteBuffer key) {
        throw new UnsupportedOperationException("Table is immutable");
    }

    /**
     * Perform table clearing.
     *
     * @throws UnsupportedOperationException if table is immutable
     */
    default void clear() {
        throw new UnsupportedOperationException("Table is immutable");
    }

    /**
     * Flush table entries to the specified channel.
     *
     * <p>File will contain:
     * <ul>
     * <li> Table entries (mapped to bytes using ByteBufferUtils.fromTableEntry)
     * <li> List of offsets (represented by int value), one for each entry
     * <li> Number of entries (represented by int value)
     * <li> Magic number in the end of the file (int Table.MAGIC)
     * </ul>
     *
     * @param channel channel to write entries to
     * @throws IOException if cannot write data
     */
    static void flushEntries(
            @NotNull final Iterator<TableEntry> entries,
            @NotNull final FileChannel channel) throws IOException {
        final var offsets = new ArrayList<Integer>();
        var offset = 0;
        while (entries.hasNext()) {
            offsets.add(offset);
            final var buffer = ByteBufferUtils.fromTableEntry(entries.next());
            offset += buffer.remaining();
            channel.write(buffer);
        }

        final var offsetsBuffer = ByteBufferUtils.fromIntList(offsets);
        channel.write(offsetsBuffer);

        final var sizeBuffer = ByteBufferUtils.fromInt(offsets.size());
        channel.write(sizeBuffer);

        final var magicBuffer = ByteBufferUtils.fromInt(SSTable.MAGIC);
        channel.write(magicBuffer);
    }

    /**
     * Read table from the specified channel.
     *
     * @param channel channel to read entries from
     * @return a table instance
     * @throws IOException if cannot read data or table format is invalid
     */
    @NotNull
    static Table from(@NotNull final FileChannel channel) throws IOException {
        if (channel.size() < Integer.BYTES) {
            throw new IOException(SSTable.INVALID_FORMAT);
        }
        final var mapped = channel.map(READ_ONLY, 0, channel.size()).order(BIG_ENDIAN);

        final var magic = mapped.getInt(mapped.limit() - Integer.BYTES);
        if (magic != SSTable.MAGIC) {
            throw new IOException(SSTable.INVALID_FORMAT);
        }

        final var entriesCount = mapped.getInt(mapped.limit() - Integer.BYTES * 2);
        if (entriesCount <= 0 || mapped.limit() < Integer.BYTES + Integer.BYTES * entriesCount) {
            throw new IOException(SSTable.INVALID_FORMAT);
        }

        final var offsets = mapped.duplicate()
                .position(mapped.limit() - Integer.BYTES * 2 - Integer.BYTES * entriesCount)
                .limit(mapped.limit() - Integer.BYTES)
                .slice()
                .asReadOnlyBuffer()
                .asIntBuffer();
        final var entries = mapped.duplicate()
                .position(0)
                .limit(mapped.limit() - Integer.BYTES * 2 - Integer.BYTES * entriesCount)
                .slice()
                .asReadOnlyBuffer();

        return new SSTable(entriesCount, offsets, entries);
    }
}
