package org.apache.ignite.internal.processors.cache.persistence.recovery.finder.filedescriptors;

import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.Finder.FileDescriptor;

public interface NodeStartDescriptor extends FileDescriptor {

    long startTime();
}
