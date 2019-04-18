package ru.mail.polis.vaddya;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
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
        try {
            final var res = ByteBuffer.allocate(size);
            final var channel = new FileInputStream(file).getChannel();
            channel.position(offset).read(res);
            return res;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
