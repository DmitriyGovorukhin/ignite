package org.apache.ignite.internal.processors.cache.persistence.recovery.finder;

public interface PageStoreDescriptor extends StoreDescriptor {
    int pageSize();
}
