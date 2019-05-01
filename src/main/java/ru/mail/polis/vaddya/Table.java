package ru.mail.polis.vaddya;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Iterator;

public interface Table {
    /**
     * Get iterator over the table starting from the given key.
     */
    @NotNull
    Iterator<TableEntry> iterator(@NotNull ByteBuffer from);

    /**
     * Insert a value into the table using the given key.
     */
    void upsert(@NotNull final ByteBuffer key,
                @NotNull final ByteBuffer value);

    /**
     * Remove a value from the table using the given key.
     */
    void remove(@NotNull final ByteBuffer key);
}
