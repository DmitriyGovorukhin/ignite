package org.apache.ignite.plugin.recovery.store;

import java.io.File;
import java.io.IOException;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;

public interface PageStoreFactory {
    PageStore createStore(File file) throws IOException;

    static PageStoreFactory create() {
        return new FilePageStoreFactory(new RandomAccessFileIOFactory());
    }
}
