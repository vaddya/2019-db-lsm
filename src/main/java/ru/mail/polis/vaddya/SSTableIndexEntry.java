package ru.mail.polis.vaddya;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;

public final class SSTableIndexEntry implements TableEntry {
    private final ByteBuffer key;
    private final ByteBuffer value;
    private final LocalDateTime ts;
    private final boolean deleted;

    @NotNull
    public static SSTableIndexEntry from(@NotNull final ByteBuffer key,
                                         @NotNull final ByteBuffer value,
                                         @NotNull final LocalDateTime ts,
                                         final boolean deleted) {
        return new SSTableIndexEntry(key, value, ts, deleted);
    }

    private SSTableIndexEntry(@NotNull final ByteBuffer key,
                              @NotNull final ByteBuffer value,
                              @NotNull final LocalDateTime ts,
                              final boolean deleted) {
        this.key = key;
        this.ts = ts;
        this.value = value;
        this.deleted = deleted;
    }

    @Override
    @NotNull
    public ByteBuffer getKey() {
        return key;
    }

    @Override
    @NotNull
    public LocalDateTime ts() {
        return ts;
    }

    @Override
    @NotNull
    public ByteBuffer getValue() {
        return value;
    }

    @Override
    public boolean isDeleted() {
        return deleted;
    }
}
