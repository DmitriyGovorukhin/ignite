package org.apache.ignite.plugin.recovery.scan;

import java.io.IOException;
import org.apache.ignite.plugin.recovery.store.PageStore;
import org.apache.ignite.plugin.recovery.store.PageStoreFactory;

public interface PageStoreScanner {

    void addHandler(ScanElement e);

    void scan() throws IOException;

    static PageStoreScanner create(String path) throws IOException {
        return create(PageStoreFactory.create().createStore(path));
    }

    static PageStoreScanner create(PageStore pageStore) throws IOException {
        return new FilePageStoreScanner(pageStore);
    }
}
