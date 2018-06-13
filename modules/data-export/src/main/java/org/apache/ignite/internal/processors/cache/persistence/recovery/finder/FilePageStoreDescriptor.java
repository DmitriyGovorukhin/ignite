package org.apache.ignite.internal.processors.cache.persistence.recovery.finder;

import java.io.File;

public interface FilePageStoreDescriptor extends PageStoreDescriptor {
    File file();
}
