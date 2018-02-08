package org.apache.ignite.plugin.recovery;

import java.io.File;
import java.util.Random;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

public class RecoveryTest extends GridCommonAbstractTest {
    private static final String CONSISTENT_ID = "NODE";

    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    private static final String CACHE_NAME = "cache";

    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setConsistentId(CONSISTENT_ID);

        cfg.setCacheConfiguration(
            new CacheConfiguration(CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.ATOMIC)
                .setAffinity(new RendezvousAffinityFunction(false, 8))
        );

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                )
                .setWalMode(WALMode.LOG_ONLY)
        );

        cfg.setAutoActivationEnabled(false);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        cfg.setPluginConfigurations(
            new RecoveryConfiguration()
        );

        return cfg;
    }

    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        deleteRecursively(new File(U.defaultWorkDirectory()));
    }

    public void test() throws Exception {
        IgniteEx ig = (IgniteEx)startGrid();

        ig.cluster().active(true);

        IgniteCache<Long, byte[]> cache = ig.cache(CACHE_NAME);

        Random rnd = new Random();

        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)ig.context().cache().context().database();

        dbMgr.checkpointReadLock();

        try {
            for (long i = 0; i < 10_000; i++) {
                byte[] arr = new byte[20];

                rnd.nextBytes(arr);

                cache.put(i, arr);

                if (i % 100 == 0)
                    System.err.println("Loaded:" + i);
            }

        }
        finally {
            dbMgr.checkpointReadUnlock();
        }

        stopGrid();

        System.err.println("Node restart!");

        ig = (IgniteEx)startGrid();

        assertTrue(!ig.cluster().active());

        RecoveryPlugin recovery = ig.plugin(RecoveryPlugin.PLUGIN_NAME);

        recovery.restoreDatabase().get();
    }
}
