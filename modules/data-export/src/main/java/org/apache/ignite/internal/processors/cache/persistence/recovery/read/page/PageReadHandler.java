package org.apache.ignite.internal.processors.cache.persistence.recovery.read.page;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.filedescriptors.PageStoreDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.recovery.read.ReadHandler;

public abstract class PageReadHandler implements ReadHandler<ByteBuffer> {

    protected final int pageSize;

    protected final PageStoreDescriptor descriptor;

    public PageReadHandler(PageStoreDescriptor descriptor) {
        this.descriptor = descriptor;
        this.pageSize = descriptor.pageSize();
    }

    @Override public void onComplete() {
        // No-op.
    }
}
