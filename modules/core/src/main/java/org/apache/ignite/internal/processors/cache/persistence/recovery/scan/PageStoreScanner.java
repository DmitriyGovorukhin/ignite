package org.apache.ignite.internal.processors.cache.persistence.recovery.scan;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.recovery.PageStore;

public interface PageStoreScanner {

    void addHandler(ScanElement e);

    void scan() throws IgniteCheckedException;

    static PageStoreScanner create(PageStore pageStore) {
        return new FilePageStoreScanner(pageStore);
    }
}
