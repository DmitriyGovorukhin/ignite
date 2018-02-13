package org.apache.ignite.plugin.recovery;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;

public class RecoveryUtils {

    public static FilePageStoreManager filePageStoreManager(GridKernalContext ctx) {
        return (FilePageStoreManager)ctx.cache().context().pageStore();
    }
}
