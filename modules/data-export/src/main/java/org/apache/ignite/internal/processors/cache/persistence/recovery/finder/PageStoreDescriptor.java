package org.apache.ignite.internal.processors.cache.persistence.recovery.finder;

public interface PageStoreDescriptor {
    int partitionId();

    long size();

    int pageSize();

    int version();

    byte type();

    String cacheOrGroupName();
}
