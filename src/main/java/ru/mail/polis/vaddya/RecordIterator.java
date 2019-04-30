package ru.mail.polis.vaddya;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static com.google.common.collect.Iterators.mergeSorted;
import static java.util.Comparator.comparing;
import static ru.mail.polis.Iters.collapseEquals;

public class RecordIterator implements Iterator<Record> {
    private static final Comparator<TableEntry> comparator = comparing(TableEntry::getKey)
            .thenComparing(comparing(TableEntry::ts).reversed());

    private final Iterator<TableEntry> iterator;

    /**
     * Iterator to merge multiple Record providers & collapse equal keys.
     *
     * @param iterators iterators to be merged
     */
    @SuppressWarnings("UnstableApiUsage")
    public RecordIterator(@NotNull final Iterable<Iterator<TableEntry>> iterators) {
        iterator = collapseEquals(mergeSorted(iterators, comparator));
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public Record next() {
        final var next = iterator.next();
        if (next.hasTombstone()) {
            throw new NoSuchElementException();
        }
        return Record.of(next.getKey(), next.getValue());
    }
}