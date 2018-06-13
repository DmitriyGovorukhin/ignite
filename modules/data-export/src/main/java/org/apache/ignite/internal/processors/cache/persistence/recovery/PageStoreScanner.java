package org.apache.ignite.internal.processors.cache.persistence.recovery;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.recovery.scan.PartitionPageStoreScanner;
import org.apache.ignite.internal.processors.cache.persistence.recovery.scan.ScanElement;
import org.apache.ignite.internal.processors.cache.persistence.recovery.stores.PartitionPageStore;

public interface PageStoreScanner {

    void addHandler(ScanElement e);

    void scan() throws IgniteCheckedException;

    static PageStoreScanner create(PartitionPageStore partitionPageStore) {
        return new PartitionPageStoreScanner(partitionPageStore);
    }
}
