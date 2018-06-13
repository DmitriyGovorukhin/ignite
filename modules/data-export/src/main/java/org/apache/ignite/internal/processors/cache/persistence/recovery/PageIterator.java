package org.apache.ignite.internal.processors.cache.persistence.recovery;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface PageIterator {
    boolean hasNext();

    int next(ByteBuffer buf) throws IOException;
}
