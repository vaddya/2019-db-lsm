package ru.mail.polis.vaddya;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;

public class DAOImpl implements DAO {
    private static final String TEMP_SUFFIX = ".tmp";
    private static final String FINAL_SUFFIX = ".db";

    private final MemTable memTable = new MemTable();
    private final List<SSTable> ssTables = new ArrayList<>();
    private final File root;
    private final long flushThreshold;

    /**
     * Creates persistent DAO.
     *
     * @param root folder to save data
     * @throws IOException if cannot read saved data
     */
    public DAOImpl(@NotNull final File root, final long flushThreshold) throws IOException {
        this.root = root;
        this.flushThreshold = flushThreshold;

        final var names = Optional.ofNullable(root.list())
                .map(Arrays::asList)
                .orElse(emptyList());
        for (final var name : names) {
            this.ssTables.add(parseSSTable(name));
        }
    }

    @NotNull
    private SSTable parseSSTable(@NotNull final String name) throws IOException {
        try (final var channel = openChannel(name, StandardOpenOption.READ)) {
            return SSTable.from(channel);
        }
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) {
        final var iterators = ssTables.stream()
                .map(table -> table.iterator(from))
                .collect(Collectors.toList());
        iterators.add(memTable.iterator(from));

        return new RecordIterator(iterators);
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key,
                       @NotNull final ByteBuffer value) throws IOException {
        memTable.upsert(key.duplicate().asReadOnlyBuffer(), value.duplicate().asReadOnlyBuffer());
        if (memTable.getCurrentSize() > flushThreshold) {
            flushMemTable();
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        memTable.remove(key.duplicate().asReadOnlyBuffer());
    }

    @Override
    public void close() throws IOException {
        flushMemTable();
    }

    private void flushMemTable() throws IOException {
        final var now = LocalDateTime.now().toString();
        final var tempName = now + TEMP_SUFFIX;
        try (final var channel = openChannel(tempName, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            memTable.flushTo(channel);
        }
        final var finalName = now + FINAL_SUFFIX;
        Files.move(path(tempName), path(finalName), StandardCopyOption.ATOMIC_MOVE);
        ssTables.add(parseSSTable(finalName));
    }

    @NotNull
    private FileChannel openChannel(@NotNull final String name,
                                    @NotNull final OpenOption... options) throws IOException {
        return FileChannel.open(path(name), options);
    }

    @NotNull
    private Path path(@NotNull final String name) {
        return Path.of(root.getAbsolutePath(), name);
    }
}
