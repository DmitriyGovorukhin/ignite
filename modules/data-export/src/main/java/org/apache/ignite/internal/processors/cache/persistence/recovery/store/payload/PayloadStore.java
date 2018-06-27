package org.apache.ignite.internal.processors.cache.persistence.recovery.store.payload;

import java.io.IOException;
import java.util.function.Consumer;
import org.apache.ignite.internal.processors.cache.persistence.recovery.model.Payload;
import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.filedescriptors.PageStoreDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.recovery.store.Store;

public class PayloadStore implements Store<Consumer<Payload>> {
    @Override public PayloadIterator iterator() {
        return null;
    }

    @Override public void read(long idx, Consumer<Payload> consumer) throws IOException {

    }

    @Override public PageStoreDescriptor descriptor() {
        return null;
    }
}
