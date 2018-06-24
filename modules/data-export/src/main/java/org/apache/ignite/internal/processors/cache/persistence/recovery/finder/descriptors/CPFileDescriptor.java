package org.apache.ignite.internal.processors.cache.persistence.recovery.finder.descriptors;

import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.Finder.Descriptor;

public interface CPFileDescriptor extends Descriptor {
    long startTime();
    long endTime();
}
