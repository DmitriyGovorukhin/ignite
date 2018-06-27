package org.apache.ignite.internal.processors.cache.persistence.recovery.finder.filedescriptors;

import java.util.UUID;
import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.Finder.FileDescriptor;

public interface CPDescriptor extends FileDescriptor {
    long time();

    String cpType();

    UUID id();
}
