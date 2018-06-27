package org.apache.ignite.internal.processors.cache.persistence.recovery.scan.handlers.extractor;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.Finder;
import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.NodeFilesFinder;
import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.filedescriptors.PageStoreDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.recovery.read.page.handlers.extractor.FrameChainBuilder;
import org.apache.ignite.internal.processors.cache.persistence.recovery.read.page.handlers.extractor.KeyValueExtractor;
import org.apache.ignite.internal.processors.cache.persistence.recovery.store.page.PartitionPageStore;
import org.apache.ignite.internal.processors.cache.persistence.recovery.read.page.PartitionPageStoreReader;
import org.apache.ignite.internal.processors.cache.persistence.recovery.read.page.handlers.PageCounter;
import org.apache.ignite.internal.processors.cache.persistence.recovery.read.page.handlers.PagesByType;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.GridAbstractTest;
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
            new CacheConfiguration(GridAbstractTest.DEFAULT_CACHE_NAME)
                .setAffinity(
                    new RendezvousAffinityFunction(false, 1))
        );

        return cfg;
    }

    public void test() throws Exception {
        Ignite ig = startGrid();

        ig.cluster().active(true);

        IgniteCache<Integer, Integer> cache = ig.cache(GridAbstractTest.DEFAULT_CACHE_NAME);

        for (int i = 0; i < 100_000; i++)
            cache.put(i, -i);

        stopGrid(0, false);

        NodeFilesFinder storeFinder = new NodeFilesFinder();

        List<Finder.FileDescriptor> stores = storeFinder.find(U.defaultWorkDirectory());

        PageStoreDescriptor desc = (PageStoreDescriptor)stores.get(1);

        PartitionPageStore partitionPageStore = new PartitionPageStore(desc,null);

        PartitionPageStoreReader scanner = new PartitionPageStoreReader(partitionPageStore);

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

        scanner.read();

        System.out.println(cnt.get());

        pagesByType.pagesByType().forEach((k, v) -> {
            System.out.println(k + " - " + v);
        });

        System.out.println("pages read: " + pageCounter.pages());
    }
}
