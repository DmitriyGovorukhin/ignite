package org.apache.ignite.internal.processors.cache.persistence.recovery.finder.descriptors;

import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.Finder.Descriptor;

public interface PageStoreDescriptor extends Descriptor {
    int partitionId();

    long size();

    int version();

    String cacheOrGroupName();

    int pageSize();
}
