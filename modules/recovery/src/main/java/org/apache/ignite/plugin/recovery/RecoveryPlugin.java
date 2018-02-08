package org.apache.ignite.plugin.recovery;

import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.plugin.IgnitePlugin;

public interface RecoveryPlugin extends IgnitePlugin {
    /** */
    public static final String PLUGIN_NAME = "GridGain";

    public RecoveryConfiguration configuration();

    public IgniteFuture<?> restoreDatabase();
}
