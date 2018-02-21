package org.apache.ignite.plugin.recovery;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface PageIterator {

    boolean hasNext();

    void next(ByteBuffer buf) throws IOException;
}
