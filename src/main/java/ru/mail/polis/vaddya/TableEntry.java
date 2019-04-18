package ru.mail.polis.vaddya;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;

public interface TableEntry {

    ByteBuffer getKey();

    ByteBuffer getValue();

    boolean isDeleted();

    LocalDateTime ts();
}
