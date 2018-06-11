package org.apache.ignite.internal.processors.cache.persistence.recovery.scan.elements;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.PageStoreDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.recovery.scan.ScanAdapter;
import org.apache.ignite.internal.processors.cache.persistence.recovery.scan.ScanElement;

public class PageCounter implements ScanElement {
    private final AtomicLong pageCounter = new AtomicLong();

    @Override public void onNextPage(ByteBuffer buf) {
        pageCounter.incrementAndGet();
    }

    @Override public void onComplete() {
        // No-op.
    }

    public long pages() {
        return pageCounter.get();
    }
}
