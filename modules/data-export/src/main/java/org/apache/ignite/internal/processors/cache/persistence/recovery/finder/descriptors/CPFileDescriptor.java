package org.apache.ignite.internal.processors.cache.persistence.recovery.finder.descriptors;

import java.util.UUID;
import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.Finder.Descriptor;

public interface CPFileDescriptor extends Descriptor {
    long time();

    String cpType();

    UUID id();
}
