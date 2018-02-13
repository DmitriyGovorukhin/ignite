package org.apache.ignite.plugin.recovery;

import java.util.List;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.plugin.IgnitePlugin;

public interface RecoveryPlugin extends IgnitePlugin {
    /** */
    public static final String PLUGIN_NAME = "GridGain";

    public RecoveryConfiguration configuration();

    public IgniteFuture<?> restoreDatabase();

    public IgniteFuture<List<FullPageId>> checkCrc();
}
