package ru.mail.polis.vaddya;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toSet;

public class DAOImpl implements DAO {
    private static final int MEM_TABLE_SIZE = 64 * 1024;
    private static final String DATA_SUFFIX = "_data.db";
    private static final String INDEX_SUFFIX = "_index.db";

    private final MemTable memTable = new MemTable();
    private final List<SSTableIndex> ssTables = new ArrayList<>();
    private final File root;

    /**
     * Creates persistent DAO.
     *
     * @param root folder to save data
     * @throws IOException if cannot read saved data
     */
    public DAOImpl(@NotNull final File root) throws IOException {
        this.root = root;

        final var names = Optional.ofNullable(root.list())
                .map(Arrays::asList)
                .orElse(emptyList())
                .stream()
                .map(file -> file.substring(0, file.lastIndexOf('_')))
                .collect(toSet());
        for (final var name : names) {
            this.ssTables.add(parseTableIndex(name));
        }
    }

    @NotNull
    private SSTableIndex parseTableIndex(@NotNull final String name) throws IOException {
        final var ts = LocalDateTime.parse(name);
        final var indexChannel = indexChannel(name, StandardOpenOption.READ);
        final var dataChannel = dataChannel(name, StandardOpenOption.READ);
        return SSTableIndex.from(ts, indexChannel, dataChannel);
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) {
        return new MergingIterator(memTable, ssTables, from);
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key,
                       @NotNull final ByteBuffer value) throws IOException {
        final var valueSize = value.remaining();
        if (memTable.getCurrentSize() + valueSize > MEM_TABLE_SIZE && memTable.getCurrentSize() > 0) {
            dumpMemTable();
        }
        memTable.upsert(key.duplicate().asReadOnlyBuffer(), Record.of(key, value));
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        memTable.remove(key.duplicate().asReadOnlyBuffer());
    }

    @Override
    public void close() throws IOException {
        dumpMemTable();
    }

    private void dumpMemTable() throws IOException {
        final var now = LocalDateTime.now().toString();
        final var indexChannel = indexChannel(now, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        final var dataChannel = dataChannel(now, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        memTable.dumpTo(indexChannel, dataChannel);
    }


    @NotNull
    private FileChannel indexChannel(@NotNull final String name,
                                     @NotNull final OpenOption... options) throws IOException {
        final var path = Path.of(root.getAbsolutePath(), name + INDEX_SUFFIX);
        return FileChannel.open(path, options);
    }

    @NotNull
    private FileChannel dataChannel(@NotNull final String name,
                                    @NotNull final OpenOption... options) throws IOException {
        final var path = Path.of(root.getAbsolutePath(), name + DATA_SUFFIX);
        return FileChannel.open(path, options);
    }
}
