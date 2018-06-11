package org.apache.ignite.internal.processors.cache.persistence.recovery.finder;

import java.io.File;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;

public interface FilePageStoreDescriptor extends PageStoreDescriptor {
    File file();

    FileIO fileIO();
}
