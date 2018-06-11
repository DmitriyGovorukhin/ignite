package org.apache.ignite.internal.processors.cache.persistence.recovery.scan;

import java.nio.ByteBuffer;

public interface ScanElement {

    void onNextPage(ByteBuffer buf);

    void onComplete();
}
