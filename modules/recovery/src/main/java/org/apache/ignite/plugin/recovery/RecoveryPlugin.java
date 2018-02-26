package org.apache.ignite.plugin.recovery;

import java.io.File;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.plugin.IgnitePlugin;

public interface RecoveryPlugin extends IgnitePlugin {
    /** */
    public static final String PLUGIN_NAME = "DG_RECOVERY";

    public RecoveryConfiguration configuration();

    public IgniteFuture<?> partitionCloning(long snapshotId, Map<String, Set<Integer>> parts, File opt);
}
