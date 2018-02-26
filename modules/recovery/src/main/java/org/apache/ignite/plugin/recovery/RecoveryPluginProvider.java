package org.apache.ignite.plugin.recovery;

import java.io.File;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.plugin.CachePluginContext;
import org.apache.ignite.plugin.CachePluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginConfiguration;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.plugin.PluginValidationException;
import org.jetbrains.annotations.Nullable;

public class RecoveryPluginProvider implements PluginProvider<RecoveryConfiguration> {
    /** Copyright. */
    private static final String COPYRIGHT = "2018 Copyright(C) Dmitriy Govorukhin";

    /** Version. */
    private static final String VER = "0.1";

    private volatile RecoveryPlugin plugin;

    @Override public String name() {
        return RecoveryPlugin.PLUGIN_NAME;
    }

    @Override public String version() {
        return VER;
    }

    @Override public String copyright() {
        return COPYRIGHT;
    }

    @SuppressWarnings("unchecked")
    @Override public <T extends IgnitePlugin> T plugin() {
        return (T)plugin;
    }

    @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) throws IgniteCheckedException {
        IgniteConfiguration icfg = ctx.igniteConfiguration();

        RecoveryConfiguration rcfg = null;

        for (PluginConfiguration pcfg : icfg.getPluginConfigurations()) {
            if (pcfg instanceof RecoveryConfiguration)
                rcfg = (RecoveryConfiguration)pcfg;
        }

        final RecoveryProcessor recoveryProcessor = new RecoveryProcessor(rcfg, ((IgniteEx)ctx.grid()).context());

        final RecoveryConfiguration finalRecoveryCfg = rcfg;

        plugin = new RecoveryPlugin() {
            @Override public RecoveryConfiguration configuration() {
                return finalRecoveryCfg;
            }

            @Override public IgniteFuture<?> partitionCloning(
                long snapshotId,
                Map<String, Set<Integer>> parts,
                File opt
            ) {
                return recoveryProcessor.clonePartitionWithDataClearing(snapshotId, parts, opt);
            }
        };
    }

    @Nullable @Override public <T> T createComponent(PluginContext ctx, Class<T> cls) {
        return null;
    }

    @Override public CachePluginProvider createCacheProvider(CachePluginContext ctx) {
        return null;
    }

    @Override public void start(PluginContext ctx) throws IgniteCheckedException {

    }

    @Override public void stop(boolean cancel) throws IgniteCheckedException {

    }

    @Override public void onIgniteStart() throws IgniteCheckedException {

    }

    @Override public void onIgniteStop(boolean cancel) {

    }

    @Nullable @Override public Serializable provideDiscoveryData(UUID nodeId) {
        return null;
    }

    @Override public void receiveDiscoveryData(UUID nodeId, Serializable data) {

    }

    @Override public void validateNewNode(ClusterNode node) throws PluginValidationException {

    }
}
