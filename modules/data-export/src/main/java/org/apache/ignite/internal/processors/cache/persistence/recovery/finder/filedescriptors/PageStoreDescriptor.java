package org.apache.ignite.internal.processors.cache.persistence.recovery.finder.filedescriptors;

import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.Finder.FileDescriptor;

public interface PageStoreDescriptor extends FileDescriptor {
    int partitionId();

    long size();

    int version();

    String cacheOrGroupName();

    int pageSize();
}
