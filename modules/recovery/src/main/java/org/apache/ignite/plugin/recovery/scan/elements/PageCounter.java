package org.apache.ignite.plugin.recovery.scan.elements;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

public class PageCounter extends ScanAdapter {
    private final AtomicLong pageCounter = new AtomicLong();

    @Override public void onNextPage(ByteBuffer buf) {
        pageCounter.incrementAndGet();
    }

    public long pages() {
        return pageCounter.get();
    }
}
