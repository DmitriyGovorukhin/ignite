package org.apache.ignite.internal.processors.cache.persistence.recovery;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface PageStore {
    PageIterator iterator(long pageLowBound, long pageHighBound);

    PageIterator iterator();

    int readPage(long pageId, ByteBuffer buf) throws IOException;

    int pageSize();
}
