package org.apache.ignite.internal.processors.cache.persistence.recovery.read;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.recovery.read.page.PartitionPageStoreReader;
import org.apache.ignite.internal.processors.cache.persistence.recovery.read.payload.PayloadStoreReader;
import org.apache.ignite.internal.processors.cache.persistence.recovery.store.page.PartitionPageStore;
import org.apache.ignite.internal.processors.cache.persistence.recovery.store.payload.PayloadStore;

public interface StoreReader<T> {

    void addHandler(ReadHandler<T> e);

    void read() throws IgniteCheckedException;

    static PartitionPageStoreReader create(PartitionPageStore partitionPageStore) {
        return new PartitionPageStoreReader(partitionPageStore);
    }

    static PayloadStoreReader create(PayloadStore payloadStore){
        return new PayloadStoreReader(payloadStore);
    }
}
