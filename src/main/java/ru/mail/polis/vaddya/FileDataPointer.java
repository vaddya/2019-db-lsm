package ru.mail.polis.vaddya;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

public class FileDataPointer {
    private final File file;
    private final int offset;
    private final int size;

    public FileDataPointer(File file, int offset, int size) {
        this.file = file;
        this.offset = offset;
        this.size = size;
    }

    public ByteBuffer read() {
        final var res = ByteBuffer.allocate(size);
        try (var is = new FileInputStream(file)) {
            is.getChannel().position(offset).read(res);
            return res;
        } catch (IOException e) {
            e.printStackTrace();
            throw new UncheckedIOException(e);
        }
    }
}
