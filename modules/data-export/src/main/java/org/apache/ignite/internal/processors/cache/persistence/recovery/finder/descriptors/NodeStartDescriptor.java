package org.apache.ignite.internal.processors.cache.persistence.recovery.finder.descriptors;

import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.Finder.Descriptor;

public interface NodeStartDescriptor extends Descriptor {

    long startTime();
}
