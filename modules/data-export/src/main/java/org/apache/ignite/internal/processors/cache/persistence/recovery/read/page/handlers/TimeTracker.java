package org.apache.ignite.internal.processors.cache.persistence.recovery.read.page.handlers;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.processors.cache.persistence.recovery.read.ReadHandler;

public class TimeTracker implements ReadHandler<ByteBuffer> {
    private boolean init;

    private long startTime;

    private long endTime;

    @Override public void onNextRead(ByteBuffer o) {
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
