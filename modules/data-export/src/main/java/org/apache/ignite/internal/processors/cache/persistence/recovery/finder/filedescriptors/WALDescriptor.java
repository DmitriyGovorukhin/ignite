package org.apache.ignite.internal.processors.cache.persistence.recovery.finder.filedescriptors;

import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.Finder.FileDescriptor;

public interface WALDescriptor extends FileDescriptor {
    long segmentIndex();
}
