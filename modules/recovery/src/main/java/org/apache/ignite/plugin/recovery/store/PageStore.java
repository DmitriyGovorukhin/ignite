package org.apache.ignite.plugin.recovery.store;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface PageStore {

    int pageSize();

    PageIterator iterator();

    void readPage(long pageId, ByteBuffer buf) throws IOException;
}
