package org.apache.ignite.plugin.recovery.scan;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.LinkedList;
import java.util.List;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.plugin.recovery.store.PageIterator;
import org.apache.ignite.plugin.recovery.store.PageStore;

public class FilePageStoreScanner implements PageStoreScanner {
    private final List<ScanElement> scanElements = new LinkedList<>();

    private final PageStore pageStore;

    public FilePageStoreScanner(PageStore pageStore) {
        this.pageStore = pageStore;
    }

    @Override public void addHandler(ScanElement e) {
        scanElements.add(e);
    }

    @Override public void scan() throws IOException {
        int pageSize = pageStore.pageSize();

        PageIterator it = pageStore.iterator();

        ByteBuffer buf = ByteBuffer.allocate(pageSize);

        buf.order(ByteOrder.nativeOrder());

        ByteBuffer tmp = GridUnsafe.allocateBuffer(pageSize);

        tmp.order(ByteOrder.nativeOrder());

        while (it.hasNext()) {
            it.next(buf);

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

        GridUnsafe.freeBuffer(tmp);
    }
}
