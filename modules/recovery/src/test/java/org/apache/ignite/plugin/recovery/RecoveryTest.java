package org.apache.ignite.plugin.recovery;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.processors.cache.persistence.file.AsyncFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.ignite.plugin.recovery.RecoveryUtils.filePageStoreManager;

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
                .setAffinity(new RendezvousAffinityFunction(false, 1))
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
        IgniteEx ig = startGrid(0);

        ig.cluster().active(true);

        long entries = 10_000;

        try (IgniteDataStreamer<Long, byte[]> st = ig.dataStreamer(CACHE_NAME)) {
            st.allowOverwrite(true);

            Random rnd = new Random();

            byte[] buff = new byte[40];

            for (long i = 0; i < entries; i++) {
                rnd.nextBytes(buff);

                st.addData(i, buff);
            }
        }

        stopGrid(0, false);

        log.info("Node restarted!!!");

        ig = startGrid(0);

        assertTrue(!ig.cluster().active());

        RecoveryPlugin recovery = ig.plugin(RecoveryPlugin.PLUGIN_NAME);

        IgniteFuture<List<FullPageId>> futCheckCrc0 = recovery.checkCrc();

        List<FullPageId> corruptedPages0 = futCheckCrc0.get();

        assertTrue("Corrupted pages count:" + corruptedPages0.size(), corruptedPages0.isEmpty());

        int pageToCorrupt = 30;

        randomCorruptPage(pageToCorrupt, ig);

        IgniteFuture<List<FullPageId>> futCheckCrc1 = recovery.checkCrc();

        List<FullPageId> corruptedPages1 = futCheckCrc1.get();

        assertEquals(pageToCorrupt, corruptedPages1.size());
    }

    private void randomCorruptPage(int pageToCorrupt, IgniteEx ig) throws IOException {
        FilePageStoreManager pageStoreManager = filePageStoreManager(ig.context());

        Path partition0 = pageStoreManager.getPath(false, CACHE_NAME, 0);

        AsyncFileIOFactory ioFactory = new AsyncFileIOFactory();

        FileIO file = ioFactory.create(partition0.toFile(), WRITE);

        int pageSize = ig.configuration().getDataStorageConfiguration().getPageSize();

        ByteBuffer buf = ByteBuffer.allocate(pageSize);

        buf.order(ByteOrder.nativeOrder());

        int initPos = pageSize;
        int size = (int)file.size();

        int bound = size / initPos;

        Set<Integer> allReadyCorrupted = new HashSet<>();

        Random rnd = new Random();

        while (pageToCorrupt > 0) {
            int idx;

            do {
                idx = rnd.nextInt(bound);
            }
            while (allReadyCorrupted.contains(idx));

            allReadyCorrupted.add(idx);

            PageIO.setCrc(buf, -42);

            int off = initPos + (pageSize * idx);

            System.out.println("offset:" + off);

            file.write(buf, off);

            pageToCorrupt--;

            buf.clear();
        }

        file.force();

    }
}
