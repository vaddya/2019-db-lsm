package ru.mail.polis.vaddya;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;

import static ru.mail.polis.vaddya.ByteUtils.emptyBuffer;

public final class TableEntry {
    private final ByteBuffer key;
    private final ByteBuffer value;
    private final boolean hasTombstone;
    private final LocalDateTime ts;

    @NotNull
    public static TableEntry upsert(@NotNull final Record record) {
        return new TableEntry(record.getKey(), record.getValue(), false, LocalDateTime.now());
    }

    @NotNull
    public static TableEntry delete(@NotNull final ByteBuffer key) {
        return new TableEntry(key, emptyBuffer(), true, LocalDateTime.now());
    }

    @NotNull
    public static TableEntry from(@NotNull final ByteBuffer key,
                                  @NotNull final ByteBuffer value,
                                  final boolean hasTombstone,
                                  @NotNull final LocalDateTime ts) {
        return new TableEntry(key, value, hasTombstone, ts);
    }

    private TableEntry(@NotNull final ByteBuffer key,
                       @NotNull final ByteBuffer value,
                       final boolean hasTombstone,
                       @NotNull final LocalDateTime ts) {
        this.key = key;
        this.value = value;
        this.hasTombstone = hasTombstone;
        this.ts = ts;
    }

    @NotNull
    public ByteBuffer getKey() {
        return key;
    }

    @NotNull
    public ByteBuffer getValue() {
        return value;
    }

    public boolean hasTombstone() {
        return hasTombstone;
    }

    @NotNull
    public LocalDateTime ts() {
        return ts;
    }
}
