package ru.mail.polis.vaddya;

import com.google.common.collect.PeekingIterator;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import static com.google.common.collect.Iterators.mergeSorted;
import static com.google.common.collect.Iterators.peekingIterator;
import static java.util.Comparator.comparing;

public class MergingIterator implements Iterator<Record> {
    private final PeekingIterator<? extends TableEntry> iterator;

    /**
     * Iterator to merge multiple Record providers.
     *
     * @param memTable current mem table
     * @param ssTables list of sorted strings tables
     * @param from     start key
     */
    @SuppressWarnings("UnstableApiUsage")
    public MergingIterator(@NotNull final MemTable memTable,
                           @NotNull final Collection<SSTableIndex> ssTables,
                           @NotNull final ByteBuffer from) {
        Collection<Iterator<? extends TableEntry>> iterators = new ArrayList<>();
        iterators.add(peekingIterator(memTable.iteratorFrom(from)));
        ssTables.stream()
                .map(table -> table.iteratorFrom(from))
                .forEach(iterators::add);

        iterator = peekingIterator(mergeSorted(iterators, comparing(TableEntry::getKey).thenComparing(TableEntry::ts)));

    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public Record next() {
        TableEntry next = iterator.next();
        while (next.isDeleted()) { // find next non-tombstone
            next = iterator.next();
        }

        while (iterator.hasNext() && iterator.peek().getKey().equals(next.getKey())) { // find overrides
            TableEntry another = iterator.next();
            if (another.isDeleted()) {
                next = iterator.next();
            } else {
                next = another;
            }
        }

        return Record.of(next.getKey(), next.getValue().position(0));
    }
}