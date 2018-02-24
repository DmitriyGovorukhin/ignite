package org.apache.ignite.plugin.recovery.scan;

import java.nio.ByteBuffer;

public interface ScanElement {

    void onNextPage(ByteBuffer buf);

    void onComplete();
}
