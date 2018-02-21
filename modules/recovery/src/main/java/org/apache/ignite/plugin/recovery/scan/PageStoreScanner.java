package org.apache.ignite.plugin.recovery.scan;

import java.io.IOException;
import org.apache.ignite.plugin.recovery.store.PageStoreFactory;

public interface PageStoreScanner {

    void addElement(ScanElement e);

    void scan() throws IOException;

    static PageStoreScanner create(String path) throws IOException {
        return new FilePageStoreScanner(PageStoreFactory.create().createStore(path));
    }
}
