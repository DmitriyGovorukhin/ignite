package org.apache.ignite.internal.processors.cache.persistence.recovery.scan;

import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.descriptors.PageStoreDescriptor;

public abstract class ScanAdapter implements ScanElement {

    protected final int pageSize = 0;

    protected final PageStoreDescriptor descriptor;

    public ScanAdapter(PageStoreDescriptor descriptor) {
        this.descriptor = descriptor;
    //    this.pageSize = descriptor.pageSize();
    }

    @Override public void onComplete() {
        // No-op.
    }
}
