package org.apache.ignite.plugin.recovery;

import java.io.IOException;
import java.util.Set;
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

        IgniteCache<Integer, String> cache = ig.cache("cache");

        for (int i = 100; i < 110; i++)
            cache.put(i, "fasfdgtrgascnjmkiul9ojhrthuk89o89eg" + i);

        // cache.get(100);

        ig.cluster().active(false);

        ig.close();
    }

    @Test
    public void test() throws IOException {
        PageStoreScanner scanner = PageStoreScanner.create(FILE);

        PageCounter pageCounter = new PageCounter();

        PagesByType pagesByType = new PagesByType();

        DataPayloadExtractor extractor = new DataPayloadExtractor();

        scanner.addElement(pagesByType);
        scanner.addElement(pageCounter);
        scanner.addElement(extractor);

        scanner.scan();

        System.out.println("Pages by type (" + pageCounter.pages() + ")");

        pagesByType.pagesByType().forEach((k, v) -> System.out.println(v + " - " + strType(k)));

        Set<DataPayloadExtractor.KeyValue> keyValueSet = extractor.keyValues();

        System.out.println("Page content (" + keyValueSet.size() + ")");

        keyValueSet.forEach(kv -> {
            System.out.println(
                "keyType:" + kv.keyType + " keyLen:" + kv.keyBytes.length +
                    " valueType:" + kv.valType + " valueLen:" + kv.valBytes.length);
        });
    }
}
