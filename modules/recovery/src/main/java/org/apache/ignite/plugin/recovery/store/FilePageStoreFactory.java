package org.apache.ignite.plugin.recovery.store;

import java.io.File;
import java.io.IOException;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;

public class FilePageStoreFactory implements PageStoreFactory {

    private final FileIOFactory fileIOFactory;

    public FilePageStoreFactory(FileIOFactory factory) {
        fileIOFactory = factory;
    }

    @Override public PageStore createStore(File file) throws IOException {
        return new FilePageStore(file, fileIOFactory);
    }
}
