package org.apache.ignite.internal.processors.cache.persistence.recovery.finder.filedescriptors;

import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.Finder.FileDescriptor;

public interface IndexStoreDescriptor extends FileDescriptor {
    long size();

    int version();

    int pageSize();
}
