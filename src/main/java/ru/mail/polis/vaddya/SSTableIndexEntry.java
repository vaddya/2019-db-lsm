package ru.mail.polis.vaddya;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;

public final class SSTableIndexEntry implements TableEntry {
    private final ByteBuffer key;
    private final LocalDateTime ts;
    private final FileDataPointer data;
    private final boolean deleted;

    @NotNull
    public static SSTableIndexEntry from(@NotNull final ByteBuffer key,
                                         @NotNull final LocalDateTime ts,
                                         @NotNull final FileDataPointer data,
                                         final boolean deleted) {
        return new SSTableIndexEntry(key, ts, data, deleted);
    }

    private SSTableIndexEntry(@NotNull final ByteBuffer key,
                              @NotNull final LocalDateTime ts,
                              @NotNull final FileDataPointer data,
                              final boolean deleted) {
        this.key = key;
        this.ts = ts;
        this.data = data;
        this.deleted = deleted;
    }

    @Override
    public ByteBuffer getKey() {
        return key;
    }

    @Override
    public LocalDateTime ts() {
        return ts;
    }

    @Override
    public ByteBuffer getValue() {
        return data.read();
    }

    @Override
    public boolean isDeleted() {
        return deleted;
    }
}
