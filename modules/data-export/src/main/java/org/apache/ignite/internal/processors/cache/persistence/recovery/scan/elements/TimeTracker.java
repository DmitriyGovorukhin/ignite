package org.apache.ignite.internal.processors.cache.persistence.recovery.scan.elements;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.processors.cache.persistence.recovery.scan.ScanElement;

public class TimeTracker implements ScanElement {
    private boolean init;

    private long startTime;

    private long endTime;

    @Override public void onNextPage(ByteBuffer buf) {
        if (!init) {
            startTime = System.currentTimeMillis();

            init = true;
        }
    }

    @Override public void onComplete() {
        endTime = System.currentTimeMillis();
    }

    public long startTime(){
        return startTime;
    }

    public long endTime() {
        return endTime;
    }
}
