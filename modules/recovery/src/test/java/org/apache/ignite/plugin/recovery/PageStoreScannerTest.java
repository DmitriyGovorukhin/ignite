package org.apache.ignite.plugin.recovery;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.plugin.recovery.scan.PageStoreScanner;
import org.apache.ignite.plugin.recovery.scan.elements.DataPayloadExtractor;
import org.apache.ignite.plugin.recovery.scan.elements.FrameChainBuilder;
import org.apache.ignite.plugin.recovery.scan.elements.KeyValue;
import org.apache.ignite.plugin.recovery.scan.elements.KeyValueExtractor;
import org.apache.ignite.plugin.recovery.scan.elements.PageCounter;
import org.apache.ignite.plugin.recovery.scan.elements.PagesByType;
import org.apache.ignite.plugin.recovery.store.PageStore;
import org.apache.ignite.plugin.recovery.store.PageStoreFactory;
import org.junit.Test;

import static org.apache.ignite.plugin.recovery.utils.PageTypeMapping.strType;

public class PageStoreScannerTest {
    private static final String FILE = "/home/dgovorukhin/workspace/projects/incubator-ignite/work/db/node/cache-cache/part-0.bin";

    @Test
    public void preload() {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setConsistentId("node");

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                )
        );

        cfg.setCacheConfiguration(
            new CacheConfiguration("cache")
                .setAffinity(
                    new RendezvousAffinityFunction(false, 1)
                )
        );

        cfg.setPluginConfigurations(
            new RecoveryConfiguration()
        );

        Ignite ig = Ignition.start(cfg);

        ig.cluster().active(true);

        IgniteCache<Integer, byte[]> cache = ig.cache("cache");

        Random rnd = new Random();

        int keys = 50_000;

        int maxLen = (4096 + 2048) * 17;

        // List<Integer> lens = new ArrayList<>(keys);

        try (IgniteDataStreamer<Integer, byte[]> st = ig.dataStreamer("cache")) {
            st.allowOverwrite(true);

            for (int i = 0; i < keys; i++) {
                byte[] bytes = new byte[rnd.nextInt(maxLen)];

                rnd.nextBytes(bytes);

                st.addData(i, bytes);

                //lens.add(bytes.length);

                if (i % 1000 == 0)
                    System.out.println("loaded:" + i);
            }
        }

        // lens.forEach(System.out::println);

        ig.cluster().active(false);

        ig.close();
    }

    @Test
    public void test() throws IOException {
        PageStoreFactory f = PageStoreFactory.create();

        PageStore store = f.createStore(new File(FILE));

        PageStoreScanner scanner = PageStoreScanner.create(store);

        PageCounter pageCounter = new PageCounter();

        PagesByType pagesByType = new PagesByType();

        FrameChainBuilder frameChainBuilder = new FrameChainBuilder();

        Set<byte[]> payloadSet = new HashSet<>();

        DataPayloadExtractor payloadExtractor = new DataPayloadExtractor(
            payloadSet::add, frameChainBuilder);

        Set<KeyValue> keyValueSet = new HashSet<>();

        KeyValueExtractor keyValueExtractor = new KeyValueExtractor(
            keyValueSet::add, frameChainBuilder
        );

        scanner.addHandler(pagesByType);
        scanner.addHandler(pageCounter);
        scanner.addHandler(frameChainBuilder);

        long time = System.currentTimeMillis();

        scanner.scan();

        System.out.println("Scan time: " + (System.currentTimeMillis() - time));

        System.out.println("Pages by type (" + pageCounter.pages() + ")");

        pagesByType.pagesByType().forEach((k, v) -> System.out.println(v + " - " + strType(k)));

        System.out.println("Payloads: " + payloadSet.size());

        System.out.println("KeyValues: " + keyValueSet.size());
    }
}
