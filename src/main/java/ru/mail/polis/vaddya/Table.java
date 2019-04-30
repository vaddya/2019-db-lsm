package ru.mail.polis.vaddya;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Iterator;

public interface Table {
    @NotNull
    Iterator<TableEntry> iterator(@NotNull ByteBuffer from);

    void upsert(@NotNull final ByteBuffer key,
                @NotNull final ByteBuffer value);

    void remove(@NotNull final ByteBuffer key);
}
