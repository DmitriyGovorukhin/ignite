package org.apache.ignite.internal.processors.cache.persistence.recovery.read.page.handlers;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.processors.cache.persistence.recovery.read.ReadHandler;

public class PageCounter implements ReadHandler {
    private final AtomicLong pageCounter = new AtomicLong();

    @Override public void onNextRead(Object next) {
        pageCounter.incrementAndGet();
    }

    @Override public void onComplete() {
        // No-op.
    }

    public long pages() {
        return pageCounter.get();
    }
}
