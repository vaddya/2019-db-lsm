package ru.mail.polis.vaddya;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;

public interface TableEntry {
    @NotNull
    ByteBuffer getKey();

    @NotNull
    ByteBuffer getValue();

    boolean isDeleted();

    @NotNull
    LocalDateTime ts();
}
