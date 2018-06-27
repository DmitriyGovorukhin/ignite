package org.apache.ignite.internal.processors.cache.persistence.recovery.read.page.handlers;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.filedescriptors.PageStoreDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.recovery.read.page.PageReadHandler;

public class ProgressBar extends PageReadHandler {
    private static final int BAR = 20;

    private final long ration;

    private final long totalSize;

    private long processed;

    public ProgressBar(PageStoreDescriptor desc) {
        super(desc);

        this.totalSize =  desc.version() == 2 ?
            desc.size() - desc.pageSize() :
            desc.size() - FilePageStore.HEADER_SIZE;

        ration = (totalSize / BAR);
    }

    @Override public void onNextRead(ByteBuffer buf) {
        long before = processed / ration;

        processed += pageSize;

        long after = processed / ration;

        if (after > before)
            updateProgress(after);
    }

    private void updateProgress(long barSize) {
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
        updateProgress(BAR);

        System.out.print("\n");
    }
}
