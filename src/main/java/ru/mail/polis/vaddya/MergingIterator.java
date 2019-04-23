package ru.mail.polis.vaddya;

import com.google.common.collect.PeekingIterator;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;

import java.util.Iterator;

import static com.google.common.collect.Iterators.mergeSorted;
import static com.google.common.collect.Iterators.peekingIterator;
import static java.util.Comparator.comparing;

public class MergingIterator implements Iterator<Record> {
    private final PeekingIterator<? extends TableEntry> iterator;

    /**
     * Iterator to merge multiple Record providers.
     *
     * @param iterators iterators to be merged
     */
    @SuppressWarnings("UnstableApiUsage")
    public MergingIterator(@NotNull final Iterable<? extends Iterator<TableEntry>> iterators) {
        iterator = peekingIterator(mergeSorted(iterators, comparing(TableEntry::getKey).thenComparing(TableEntry::ts)));
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public Record next() {
        var next = iterator.next();
        while (next.hasTombstone()) { // find next without tombstone
            next = iterator.next();
        }

        while (iterator.hasNext() && iterator.peek().getKey().equals(next.getKey())) { // find overrides
            final var another = iterator.next();
            if (another.hasTombstone()) {
                next = iterator.next();
            } else {
                next = another;
            }
        }

        return Record.of(next.getKey(), next.getValue().position(0));
    }
}