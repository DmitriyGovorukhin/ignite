package org.apache.ignite.plugin.recovery;

import java.io.IOException;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;

public interface PageStoreFactory {
    PageStore createStore(String path) throws IOException;

    static PageStoreFactory create() {
        return new FilePageStoreFactory(new RandomAccessFileIOFactory());
    }
}
