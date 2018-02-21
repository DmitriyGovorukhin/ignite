package org.apache.ignite.plugin.recovery.scan;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import org.apache.ignite.plugin.recovery.store.PageIterator;
import org.apache.ignite.plugin.recovery.store.PageStore;

public class FilePageStoreScanner implements PageStoreScanner {
    private final List<ScanElement> scanElements = new LinkedList<>();

    private final PageStore pageStore;

    public FilePageStoreScanner(PageStore pageStore){
        this.pageStore = pageStore;
    }

    @Override public void addElement(ScanElement e) {
        scanElements.add(e);
    }

    @Override public void scan() throws IOException {
        int pageSize = pageStore.pageSize();

        PageIterator it = pageStore.iterator();

        ByteBuffer buf = ByteBuffer.allocate(pageSize);

        ByteBuffer tmp = ByteBuffer.allocate(pageSize);

        while (it.hasNext()) {
            it.next(buf);

            for (ScanElement e : scanElements) {
                tmp.put(buf);

                e.onNextPage(tmp);

                tmp.clear();
            }

        }
    }
}
