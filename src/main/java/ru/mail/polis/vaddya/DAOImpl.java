package ru.mail.polis.vaddya;

import com.google.common.primitives.Ints;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Record;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.*;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

public class DAOImpl implements DAO {
    private static final int BLOCK_SIZE = 64 * 1024;
    private static final String DATA_SUFFIX = "_data.db";
    private static final String INDEX_SUFFIX = "_index.db";

    private final NavigableMap<ByteBuffer, Record> memtable = new TreeMap<>();
    private final List<SSTableIndex> sstables;
    private final File root;

    private int currentSize = 0;

    public DAOImpl(@NotNull final File root) throws IOException {
        this.root = root;

        this.sstables = Optional.ofNullable(root.list())
                .map(Arrays::asList)
                .orElse(emptyList())
                .stream()
                .map(file -> file.substring(0, file.lastIndexOf("_")))
                .distinct()
                .map(this::parseTableIndex)
                .collect(toList());
    }


    private SSTableIndex parseTableIndex(String name) {
        try {
            File indexFile = new File(root, name + INDEX_SUFFIX);
            File dataFile = new File(root, name + DATA_SUFFIX);

            byte[] bytes;
            bytes = Files.readAllBytes(indexFile.toPath());
            NavigableMap<ByteBuffer, SSTableIndexEntry> entries = parseIndexEntries(bytes);

            return new SSTableIndex(dataFile, entries);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @NotNull
    @Override
    public ByteBuffer get(@NotNull ByteBuffer key) throws IOException, NoSuchElementException {
        if (memtable.containsKey(key)) {
            return memtable.get(key).getValue();
        }

        for (SSTableIndex sstable : sstables) {
            if (sstable.getEntries().containsKey(key)) {
                SSTableIndexEntry index = sstable.getEntries().get(key);

                byte[] bytes = new byte[index.getSize()];
                new FileInputStream(sstable.getFile()).read(bytes, index.getOffset(), index.getSize());

                Record record = parseRecord(bytes, index.getKey().capacity());
                return record.getValue();
            }
        }
        throw new NoSuchElementException("Not found");
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull ByteBuffer from) {
        return memtable.tailMap(from).values().iterator();
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        int valueSize = value.remaining();
        if (currentSize + valueSize > BLOCK_SIZE) {
            dump();
        }
        currentSize = valueSize;
        memtable.put(key, Record.of(key, value));
    }

    private void dump() throws IOException {
        String now = LocalDateTime.now().toString();

        String dataFilename = now + DATA_SUFFIX;
        File dataFile = createFile(dataFilename);
        String indexFilename = now + INDEX_SUFFIX;
        File indexFile = createFile(indexFilename);

        Path dataPath = dataFile.toPath();
        Path indexPath = indexFile.toPath();
        int position = 0;
        for (var entry : memtable.entrySet()) {
            byte[] recordBytes = recordToBytes(entry.getValue());
            Files.write(dataPath, recordBytes);

            SSTableIndexEntry indexEntry = SSTableIndexEntry.from(entry.getKey(), position, recordBytes.length);
            byte[] indexBytes = createIndexEntry(indexEntry);
            Files.write(indexPath, indexBytes);

            position += recordBytes.length;
        }
    }

    @Override
    public void remove(@NotNull ByteBuffer key) {
        memtable.remove(key);
    }

    @Override
    public void close() throws IOException {
        dump();
    }

    @NotNull
    private byte[] createIndexEntry(@NotNull final SSTableIndexEntry indexEntry) {
        int keySize = indexEntry.getKey().remaining();
        byte[] bytes = new byte[4 + keySize + 4 + 4];
        writeIntToByteArray(bytes, keySize, 0);
        indexEntry.getKey().get(bytes, 4, keySize);
        writeIntToByteArray(bytes, indexEntry.getOffset(), 4 + keySize);
        writeIntToByteArray(bytes, indexEntry.getSize(), 4 + keySize + 4);
        return bytes;
    }

    @NotNull
    private NavigableMap<ByteBuffer, SSTableIndexEntry> parseIndexEntries(@NotNull final byte[] bytes) {
        NavigableMap<ByteBuffer, SSTableIndexEntry> indexes = new TreeMap<>();
        for (int i = 0; i < bytes.length; i++) {
            int keySize = readIntFromByteArray(bytes, i);
            i += 4;
            ByteBuffer key = ByteBuffer.allocate(keySize);
            key.put(bytes, i, keySize);
            i += keySize;
            int offset = readIntFromByteArray(bytes, i);
            i += 4;
            int size = readIntFromByteArray(bytes, i);
            i += 4;

            indexes.put(key, SSTableIndexEntry.from(key, offset, size));
        }
        return indexes;
    }

    private void writeIntToByteArray(@NotNull final byte[] dest, final int value, final int position) {
        byte[] bytes = Ints.toByteArray(value);
        assert bytes.length == 4;
        System.arraycopy(bytes, 0, dest, position, bytes.length);
    }

    private int readIntFromByteArray(@NotNull final byte[] src, final int position) {
        return Ints.fromBytes(src[position], src[position + 1], src[position + 2], src[position + 3]);
    }

    @NotNull
    private byte[] recordToBytes(@NotNull final Record record) {
        int keySize = record.getKey().remaining();
        int valueSize = record.getValue().remaining();
        byte[] bytes = new byte[keySize + valueSize];
        record.getKey().get(bytes, 0, keySize);
        record.getValue().get(bytes, keySize, valueSize);
        return bytes;
    }

    @NotNull
    private Record parseRecord(@NotNull final byte[] bytes, final int keySize) {
        byte[] key = new byte[keySize];
        System.arraycopy(bytes, 0, key, 0, keySize);
        int valueSize = bytes.length - keySize;
        byte[] value = new byte[valueSize];
        System.arraycopy(bytes, keySize, value, 0, valueSize);
        return Record.of(ByteBuffer.wrap(key), ByteBuffer.wrap(value));
    }

    @NotNull
    private File createFile(@NotNull final String filename) throws IOException {
        File file = new File(root, filename);
        if (!file.createNewFile()) {
            System.err.println("Cannot create index file: " + file.getAbsolutePath());
        }
        return file;
    }
}
