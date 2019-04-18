package ru.mail.polis.vaddya;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;

import static ru.mail.polis.vaddya.ByteUtils.emptyBuffer;

public class MemTableEntry implements TableEntry {
    private final Record record;
    private final boolean deleted;
    private final LocalDateTime ts;

    @NotNull
    public static MemTableEntry upsert(@NotNull final Record record) {
        return new MemTableEntry(record, false);
    }

    @NotNull
    public static MemTableEntry delete(@NotNull final ByteBuffer key) {
        return new MemTableEntry(Record.of(key, emptyBuffer()), true);
    }

    private MemTableEntry(@NotNull final Record record,
                          final boolean deleted) {
        this.ts = LocalDateTime.now();
        this.record = record;
        this.deleted = deleted;
    }

    @Override
    public boolean isDeleted() {
        return deleted;
    }

    @Override
    public LocalDateTime ts() {
        return ts;
    }

    @Override
    public ByteBuffer getKey() {
        return record.getKey();
    }

    @Override
    public ByteBuffer getValue() {
        return record.getValue();
    }
}
