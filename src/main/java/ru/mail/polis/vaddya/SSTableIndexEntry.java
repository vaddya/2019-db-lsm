package ru.mail.polis.vaddya;

import java.nio.ByteBuffer;

public class SSTableIndexEntry {
    private final ByteBuffer key;
    private final int offset;
    private final int size;

    public static SSTableIndexEntry from(ByteBuffer key, int offset, int size) {
        return new SSTableIndexEntry(key, offset, size);
    }

    private SSTableIndexEntry(ByteBuffer key, int offset, int size) {
        this.key = key;
        this.offset = offset;
        this.size = size;
    }

    public ByteBuffer getKey() {
        return key;
    }

    public int getOffset() {
        return offset;
    }

    public int getSize() {
        return size;
    }
}
