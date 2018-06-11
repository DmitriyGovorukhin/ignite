package org.apache.ignite.internal.processors.cache.persistence.recovery.scan.elements.extractor;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.persistence.recovery.stores.RecoveryPageStore;
import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.FilePageStoreDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.FilePageStoreFinder;
import org.apache.ignite.internal.processors.cache.persistence.recovery.scan.FilePageStoreScanner;
import org.apache.ignite.internal.processors.cache.persistence.recovery.scan.elements.PageCounter;
import org.apache.ignite.internal.processors.cache.persistence.recovery.scan.elements.PagesByType;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

public class KeyValueExtractorTest extends GridCommonAbstractTest {
    public static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanReferences();
    }

    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setConsistentId("NODE");

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                )
        );

        cfg.setCacheConfiguration(
            new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setAffinity(
                    new RendezvousAffinityFunction(false, 1))
        );

        return cfg;
    }

    public void test() throws Exception {
        Ignite ig = startGrid();

        ig.cluster().active(true);

        IgniteCache<Integer, Integer> cache = ig.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 100_000; i++)
            cache.put(i, -i);

        stopGrid(0, false);

        FilePageStoreFinder storeFinder = new FilePageStoreFinder();

        List<FilePageStoreDescriptor> stores = storeFinder.findStores(U.defaultWorkDirectory());

        FilePageStoreDescriptor desc = stores.get(1);

        RecoveryPageStore recoveryPageStore = new RecoveryPageStore(desc);

        FilePageStoreScanner scanner = new FilePageStoreScanner(recoveryPageStore);

        PageCounter pageCounter = new PageCounter();

        scanner.addHandler(pageCounter);

        PagesByType pagesByType = new PagesByType(desc);

        scanner.addHandler(pagesByType);

        FrameChainBuilder frameChainBuilder = new FrameChainBuilder(desc);

        scanner.addHandler(frameChainBuilder);

        AtomicInteger cnt = new AtomicInteger();

        KeyValueExtractor ex = new KeyValueExtractor((kv) -> {
            cnt.incrementAndGet();

        }, frameChainBuilder);

        scanner.scan();

        System.out.println(cnt.get());

        pagesByType.pagesByType().forEach((k, v) -> {
            System.out.println(k + " - " + v);
        });

        System.out.println("pages read: " + pageCounter.pages());
    }
}
