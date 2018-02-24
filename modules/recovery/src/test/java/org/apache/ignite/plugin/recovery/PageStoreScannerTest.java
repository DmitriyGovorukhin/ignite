package org.apache.ignite.plugin.recovery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.plugin.recovery.scan.PageStoreScanner;
import org.apache.ignite.plugin.recovery.scan.elements.DataPayloadExtractor;
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

        int keys = 1000;

        int maxLen = (4096 + 2048) * 17;

        List<Integer> lens = new ArrayList<>(keys);

        for (int i = 0; i < keys; i++) {
            byte[] bytes = new byte[rnd.nextInt(maxLen)];

            rnd.nextBytes(bytes);

            cache.put(i, bytes);

            lens.add(bytes.length);
        }

        lens.forEach(System.out::println);

        ig.cluster().active(false);

        ig.close();
    }

    @Test
    public void test() throws IOException {
        PageStoreFactory f = PageStoreFactory.create();

        PageStore store = f.createStore(FILE);

        PageStoreScanner scanner = PageStoreScanner.create(store);

        PageCounter pageCounter = new PageCounter();

        PagesByType pagesByType = new PagesByType();

        DataPayloadExtractor extractor = new DataPayloadExtractor();

        scanner.addHandler(pagesByType);
        scanner.addHandler(pageCounter);
        scanner.addHandler(extractor);

        scanner.scan();

        System.out.println("Pages by type (" + pageCounter.pages() + ")");

        pagesByType.pagesByType().forEach((k, v) -> System.out.println(v + " - " + strType(k)));

        Set<DataPayloadExtractor.KeyValue> keyValueSet = extractor.keyValues();

        System.out.println("Page content (" + keyValueSet.size() + ")");

        AtomicLong dataSize = new AtomicLong();

        keyValueSet.forEach(kv -> {
            dataSize.addAndGet(kv.keyBytes.length);
            dataSize.addAndGet(kv.valBytes.length);

            System.out.println(
                "keyType:" + kv.keyType + " keyLen:" + kv.keyBytes.length +
                    " valueType:" + kv.valType + " valueLen:" + kv.valBytes.length);
        });

        System.out.println("Total partition size:" + str((pageCounter.pages() * store.pageSize())));
        System.out.println("Total data size:" + str(dataSize.get()));
    }

    private static long KB = 1024;
    private static long MB = 1024 * 1024;

    private String str(long bytes) {
        long mbs = bytes / MB;

        if (mbs > 0)
            return mbs + "." + (bytes % MB) + "mb";

        return String.valueOf(bytes);
    }
}
