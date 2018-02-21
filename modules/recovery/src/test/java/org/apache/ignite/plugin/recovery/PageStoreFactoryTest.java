package org.apache.ignite.plugin.recovery;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import junit.framework.TestCase;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.PureJavaCrc32;
import org.apache.ignite.plugin.recovery.store.PageIterator;
import org.apache.ignite.plugin.recovery.store.PageStore;
import org.apache.ignite.plugin.recovery.store.PageStoreFactory;

import static java.nio.ByteBuffer.allocate;

public class PageStoreFactoryTest extends TestCase {
    public void test() throws IOException {
        PageStoreFactory pageStoreFactory = PageStoreFactory.create();

        PageStore store = pageStoreFactory.createStore("/{path}/");

        int pageSize = store.pageSize();

        ByteBuffer buf = allocate(pageSize);

        Set<Long> pages = new HashSet<>();

        PageIterator it = store.iterator();

        while (it.hasNext()) {
            it.next(buf);

            long pageId = PageIO.getPageId(buf);

            int crc = PageIO.getCrc(buf);

            PageIO.setCrc(buf, 0);

            int currCrc = PureJavaCrc32.calcCrc32(buf, pageSize);

            if (crc != currCrc)
                pages.add(pageId);
        }
    }
}
