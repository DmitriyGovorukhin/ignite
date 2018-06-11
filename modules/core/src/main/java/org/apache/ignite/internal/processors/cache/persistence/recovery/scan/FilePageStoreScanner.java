package org.apache.ignite.internal.processors.cache.persistence.recovery.scan;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.LinkedList;
import java.util.List;
import org.apache.ignite.internal.processors.cache.persistence.recovery.PageIterator;
import org.apache.ignite.internal.processors.cache.persistence.recovery.PageStore;
import org.apache.ignite.internal.util.GridUnsafe;

public class FilePageStoreScanner implements PageStoreScanner {
    private final List<ScanElement> scanElements = new LinkedList<>();

    private final PageStore pageStore;

    public FilePageStoreScanner(PageStore pageStore) {
        this.pageStore = pageStore;
    }

    @Override public void addHandler(ScanElement e) {
        scanElements.add(e);
    }

    @Override public void scan() {
        int pageSize = pageStore.pageSize();

        PageIterator it = pageStore.iterator();

        ByteBuffer buf = ByteBuffer.allocate(pageSize);

        buf.order(ByteOrder.nativeOrder());

        ByteBuffer tmp = GridUnsafe.allocateBuffer(pageSize);

        tmp.order(ByteOrder.nativeOrder());

        try {
            while (it.hasNext()) {
                try {
                    it.next(buf);
                }
                catch (IOException e) {
                    e.printStackTrace();
                }

                buf.flip();

                for (ScanElement e : scanElements) {
                    tmp.put(buf);

                    tmp.rewind();

                    e.onNextPage(tmp);

                    tmp.clear();
                }

                buf.clear();
            }

            for (ScanElement e : scanElements)
                e.onComplete();
        }
        finally {
            GridUnsafe.freeBuffer(tmp);
        }
    }
}
