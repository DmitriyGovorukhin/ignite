package org.apache.ignite.plugin.recovery;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.jupiter.api.Test;

public class ClonePartitionWithDataClearing {

    @Test
    public void test() throws IgniteCheckedException {
       String workDir = U.defaultWorkDirectory();

/*         IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setConsistentId("NODE");

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setStoragePath(workDir + "/pds")
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                )
        );

        cfg.setCacheConfiguration(
            new CacheConfiguration("myCache")
                .setAffinity(
                    new RendezvousAffinityFunction(false, 1)
                )
        );

        cfg.setPluginConfigurations(
            new RecoveryConfiguration()
        );

        Ignite ig = Ignition.start(cfg);

        ig.cluster().active(true);

        Random rnd = new Random();

        int keys = 10_000;

        int maxLen = (4096 + 2048) * 17;

        try (IgniteDataStreamer<Integer, byte[]> st = ig.dataStreamer("myCache")) {
            st.allowOverwrite(true);

            for (int i = 0; i < keys; i++) {
                byte[] bytes = new byte[rnd.nextInt(maxLen)];

                rnd.nextBytes(bytes);

                st.addData(i, bytes);

                if (i % 1000 == 0)
                    System.out.println("loaded:" + i);
            }
        }

        ig.cluster().active(false);

        ig.close();*/

        IgniteConfiguration cfg2 = new IgniteConfiguration();

        cfg2.setConsistentId("NODE");

        cfg2.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setStoragePath(workDir + "/pds")
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                )
        );

        cfg2.setCacheConfiguration(
            new CacheConfiguration("myCache")
                .setAffinity(
                    new RendezvousAffinityFunction(false, 1)
                )
        );

        cfg2.setPluginConfigurations(
            new RecoveryConfiguration()
        );

        Ignite ig2 = Ignition.start(cfg2);

        RecoveryPlugin recovery = ig2.plugin(RecoveryPlugin.PLUGIN_NAME);

        Map<String, Set<Integer>> partsMap = new HashMap<>();

        Set<Integer> parts = new HashSet<>();

        parts.add(0);

        partsMap.put("myCache", parts);

        recovery.partitionCloning(0, partsMap, new File(workDir, "out")).get();
    }
}
