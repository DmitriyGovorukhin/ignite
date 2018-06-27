package org.apache.ignite.internal.processors.cache.persistence.recovery.read.payload;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.recovery.model.Payload;
import org.apache.ignite.internal.processors.cache.persistence.recovery.read.StoreReader;
import org.apache.ignite.internal.processors.cache.persistence.recovery.read.ReadHandler;
import org.apache.ignite.internal.processors.cache.persistence.recovery.store.payload.PayloadStore;

public class PayloadStoreReader implements StoreReader<Payload> {
    private final PayloadStore payloadStore;

    public PayloadStoreReader(PayloadStore payloadStore) {
        this.payloadStore = payloadStore;
    }

    @Override public void addHandler(ReadHandler<Payload> e) {

    }

    @Override public void read() throws IgniteCheckedException {

    }
}
