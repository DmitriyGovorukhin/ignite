package org.apache.ignite.internal.processors.cache.persistence.recovery.store;

import java.io.IOException;
import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.filedescriptors.PageStoreDescriptor;

public interface Store<T> {
    StoreIterator<T> iterator();

    void read(long idx, T t) throws IOException;

    PageStoreDescriptor descriptor();
}
