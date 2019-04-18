package ru.mail.polis.vaddya;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.NavigableMap;

public class SSTableIndex {
    private final File file;
    private final NavigableMap<ByteBuffer, SSTableIndexEntry> entries;

    public SSTableIndex(File file, NavigableMap<ByteBuffer, SSTableIndexEntry> entries) {
        this.file = file;
        this.entries = entries;
    }

    public File getFile() {
        return file;
    }

    public NavigableMap<ByteBuffer, SSTableIndexEntry> getEntries() {
        return entries;
    }
}
