package ru.mail.polis.vaddya;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;

import static ru.mail.polis.vaddya.ByteBufferUtils.emptyBuffer;

public final class TableEntry {
    private final ByteBuffer key;
    @Nullable
    private final ByteBuffer value;
    private final boolean hasTombstone;
    private final long ts;

    @NotNull
    public static TableEntry upsert(@NotNull final ByteBuffer key,
                                    @NotNull final ByteBuffer value) {
        return new TableEntry(key, value, false, System.currentTimeMillis());
    }

    @NotNull
    public static TableEntry delete(@NotNull final ByteBuffer key) {
        return new TableEntry(key, emptyBuffer(), true, System.currentTimeMillis());
    }

    @NotNull
    public static TableEntry from(@NotNull final ByteBuffer key,
                                  @Nullable final ByteBuffer value,
                                  final boolean hasTombstone,
                                  final long ts) {
        return new TableEntry(key, value, hasTombstone, ts);
    }

    private TableEntry(@NotNull final ByteBuffer key,
                       @Nullable final ByteBuffer value,
                       final boolean hasTombstone,
                       final long ts) {
        this.key = key;
        this.value = value;
        this.hasTombstone = hasTombstone;
        this.ts = ts;
    }

    @NotNull
    public ByteBuffer getKey() {
        return key;
    }

    /**
     * Get the value.
     *
     * @throws IllegalArgumentException if value is absent
     */
    @NotNull
    public ByteBuffer getValue() {
        if (value == null) {
            throw new IllegalArgumentException("Value is absent");
        }
        return value;
    }

    public boolean hasTombstone() {
        return hasTombstone;
    }

    public long ts() {
        return ts;
    }
}
