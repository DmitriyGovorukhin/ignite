package org.apache.ignite.internal.processors.cache.persistence.recovery.scan.elements;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.processors.cache.persistence.recovery.scan.ScanAdapter;

public class PageCounter extends ScanAdapter {
    private final AtomicLong pageCounter = new AtomicLong();

    @Override public void onNextPage(ByteBuffer buf) {
        pageCounter.incrementAndGet();
    }

    public long pages() {
        return pageCounter.get();
    }
}
