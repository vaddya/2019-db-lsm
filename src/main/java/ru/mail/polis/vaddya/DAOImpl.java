package ru.mail.polis.vaddya;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

public class DAOImpl implements DAO {
    private static final String TEMP_SUFFIX = ".tmp";
    private static final String FINAL_SUFFIX = ".db";

    private final MemTable memTable = new MemTable();
    private final List<SSTable> ssTables;
    private final File root;
    private final long flushThresholdInBytes;

    /**
     * Creates persistent DAO.
     *
     * @param root folder to save and read data from
     * @throws UncheckedIOException if cannot open or read SSTables
     */
    public DAOImpl(@NotNull final File root, final long flushThresholdInBytes) {
        this.root = root;
        this.flushThresholdInBytes = flushThresholdInBytes;
        this.ssTables = Optional.ofNullable(root.list())
                .map(Arrays::asList)
                .orElse(emptyList())
                .stream()
                .filter(s -> s.endsWith(FINAL_SUFFIX))
                .map(this::pathTo)
                .map(this::parseSSTable)
                .collect(toList());
    }

    @NotNull
    private SSTable parseSSTable(@NotNull final Path path) {
        try (var channel = FileChannel.open(path, StandardOpenOption.READ)) {
            return SSTable.from(channel);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) {
        final var iterators = ssTables.stream()
                .map(table -> table.iterator(from))
                .collect(toList());
        iterators.add(memTable.iterator(from));

        return new RecordIterator(iterators);
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key,
                       @NotNull final ByteBuffer value) throws IOException {
        memTable.upsert(key.duplicate().asReadOnlyBuffer(), value.duplicate().asReadOnlyBuffer());
        if (memTable.getCurrentSize() > flushThresholdInBytes) {
            flushMemTable();
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key.duplicate().asReadOnlyBuffer());
        if (memTable.getCurrentSize() > flushThresholdInBytes) {
            flushMemTable();
        }
    }

    @Override
    public void close() throws IOException {
        flushMemTable();
    }

    private void flushMemTable() throws IOException {
        final var now = LocalDateTime.now().toString();
        final var tempPath = pathTo(now + TEMP_SUFFIX);
        try (var channel = FileChannel.open(tempPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            memTable.flushTo(channel);
        }

        final var finalPath = pathTo(now + FINAL_SUFFIX);
        Files.move(tempPath, finalPath, StandardCopyOption.ATOMIC_MOVE);

        final var ssTable = parseSSTable(finalPath);
        ssTables.add(ssTable);
    }

    @NotNull
    private Path pathTo(@NotNull final String name) {
        return Path.of(root.getAbsolutePath(), name);
    }
}
