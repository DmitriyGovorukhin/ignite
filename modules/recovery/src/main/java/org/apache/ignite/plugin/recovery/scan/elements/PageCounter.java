package org.apache.ignite.plugin.recovery.scan.elements;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.plugin.recovery.scan.ScanElement;

public class PageCounter implements ScanElement {
    private final AtomicLong pageCounter = new AtomicLong();

    @Override public void onNextPage(ByteBuffer buf) {
        pageCounter.incrementAndGet();
    }

    public long pages() {
        return pageCounter.get();
    }
}
