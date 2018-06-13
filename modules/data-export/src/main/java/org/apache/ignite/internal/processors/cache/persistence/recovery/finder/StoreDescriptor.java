package org.apache.ignite.internal.processors.cache.persistence.recovery.finder;

public interface StoreDescriptor {
    int partitionId();

    long size();

    int version();

    byte type();

    String cacheOrGroupName();
}
