package ru.mail.polis.vaddya;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.mail.polis.Record;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static com.google.common.collect.Iterators.peekingIterator;
import static java.util.Comparator.comparing;

public class MergingIterator implements Iterator<Record> {
    private final Collection<PeekingIterator<? extends TableEntry>> iterators = new ArrayList<>();

    /**
     * Iterator to merge multiple Record providers.
     *
     * @param memTable current mem table
     * @param ssTables list of sorted strings tables
     * @param from     start key
     */
    public MergingIterator(@NotNull final MemTable memTable,
                           @NotNull final Collection<SSTableIndex> ssTables,
                           @NotNull final ByteBuffer from) {
        iterators.add(peekingIterator(memTable.iteratorFrom(from)));
        ssTables.stream()
                .map(table -> table.iteratorFrom(from))
                .map(Iterators::peekingIterator)
                .forEach(iterators::add);
    }

    @Override
    public boolean hasNext() {
        return iterators.stream().anyMatch(Iterator::hasNext);
    }

    @Override
    public Record next() {
        while (true) {
            final var next = findNextTableEntry();
            if (next != null) {
                return Record.of(next.getKey(), next.getValue().position(0));
            }
        }
    }

    @Nullable
    private TableEntry findNextTableEntry() {
        final var next = iterators.stream()
                .filter(Iterator::hasNext)
                .min(comparing((PeekingIterator<? extends TableEntry> o) -> o.peek().getKey())
                             .thenComparing(o -> o.peek().ts()))
                .orElseThrow(NoSuchElementException::new)
                .next();
        if (next.isDeleted()) {
            return null;
        }

        final var hasNewer = iterators.stream()
                .filter(Iterator::hasNext)
                .map(PeekingIterator::peek)
                .anyMatch(entry -> entry.getKey().equals(next.getKey())
                                   && entry.ts().isAfter(next.ts()));
        if (hasNewer) {
            return null;
        }

        return next;
    }
}
