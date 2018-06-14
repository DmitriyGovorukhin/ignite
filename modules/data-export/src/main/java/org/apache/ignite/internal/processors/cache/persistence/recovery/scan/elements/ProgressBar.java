package org.apache.ignite.internal.processors.cache.persistence.recovery.scan.elements;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.ignite.internal.processors.cache.persistence.recovery.scan.ScanElement;

public class ProgressBar implements ScanElement {
    private static final int BAR = 20;

    private final long ration;

    private final long totalSize;

    private final long pageSize;

    private long processed;

    public ProgressBar(long pageSize, long totalSize) {
        this.pageSize = pageSize;
        this.totalSize = totalSize;

        ration = (totalSize / BAR);
    }

    @Override public void onNextPage(ByteBuffer buf) {
        long before = processed / ration;

        processed += pageSize;

        long after = processed / ration;

        if (after > before)
            updateBar(after);
    }

    private void updateBar(long barSize) {
        StringBuilder bar = new StringBuilder();

        for (int i = 0; i < BAR; i++) {
            if (i < barSize)
                bar.append("=");
            else
                bar.append(" ");
        }

        String data = "\r" + processed + "/" + totalSize + " [" + bar + "]";

        try {
            System.out.write(data.getBytes());
        }
        catch (IOException e) {

        }
    }

    @Override public void onComplete() {
        updateBar(BAR);

        System.out.print("\n");
    }
}
