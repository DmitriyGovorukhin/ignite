package org.apache.ignite.internal.processors.cache.persistence.recovery;

import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.FilePageStoreDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.recovery.stores.PartitionPageStore;

public interface PageStoreFactory {

    static PageStore create(FilePageStoreDescriptor desc) {
        return new PartitionPageStore(desc);
    }
}
