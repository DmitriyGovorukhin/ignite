package org.apache.ignite.internal.processors.cache.persistence.recovery.store.payload;

import java.util.function.Consumer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.recovery.model.Payload;
import org.apache.ignite.internal.processors.cache.persistence.recovery.store.StoreIterator;

public class PayloadIterator implements StoreIterator<Consumer<Payload>> {
    @Override public boolean hasNext() {
        return false;
    }

    @Override public void next(Consumer<Payload> payloadConsumer) throws IgniteCheckedException {

    }
}
