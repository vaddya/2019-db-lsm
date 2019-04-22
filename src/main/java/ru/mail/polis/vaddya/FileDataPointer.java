package ru.mail.polis.vaddya;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import static java.nio.file.StandardOpenOption.READ;

public final class FileDataPointer {
    private final Path path;
    private final int offset;
    private final int size;

    /**
     * Creates instance of pointer to data inside file.
     *
     * @param path   path to file to read from
     * @param offset offset inside the file
     * @param size   size of the data to read
     */
    @NotNull
    public static FileDataPointer to(@NotNull final Path path,
                                     final int offset,
                                     final int size) {
        return new FileDataPointer(path, offset, size);
    }

    private FileDataPointer(@NotNull final Path path,
                            final int offset,
                            final int size) {
        this.path = path;
        this.offset = offset;
        this.size = size;
    }

    /**
     * Read data from file.
     *
     * @return bytes of data
     */
    public ByteBuffer read() {
        final var res = ByteBuffer.allocate(size);
        try (final var channel = FileChannel.open(path, READ)) {
            channel.position(offset).read(res);
            return res;
        } catch (IOException e) {
            e.printStackTrace();
            throw new UncheckedIOException(e);
        }
    }
}
