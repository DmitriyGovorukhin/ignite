package org.apache.ignite.internal.processors.cache.persistence.recovery.finder;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.Finder.FileDescriptor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.processors.cache.persistence.recovery.finder.Finder.Type.CP;
import static org.apache.ignite.internal.processors.cache.persistence.recovery.finder.Finder.Type.INDEX_STORE;
import static org.apache.ignite.internal.processors.cache.persistence.recovery.finder.Finder.Type.NODE_START;
import static org.apache.ignite.internal.processors.cache.persistence.recovery.finder.Finder.Type.PAGE_STORE;
import static org.apache.ignite.internal.processors.cache.persistence.recovery.finder.Finder.Type.WAL;

public class NodeFilesFinderTest extends GridCommonAbstractTest {

    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    private static final String CONSISTENT_ID = "NODE";

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(CONSISTENT_ID);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        cfg.setCacheConfiguration(
            new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setAffinity(new RendezvousAffinityFunction(false, 32))
        );

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                )
        );

        return cfg;
    }

    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        Ignite ig = startGrid();

        ig.cluster().active(true);

        try (IgniteDataStreamer<Integer, Integer> st = ig.dataStreamer(DEFAULT_CACHE_NAME)) {
            st.allowOverwrite(true);

            for (int i = 0; i < 10_000; i++)
                st.addData(i, -i);
        }

        ig.cluster().active(false);

        stopGrid(0, false);
    }

    public void test() throws IgniteCheckedException {
        String workDirectory = U.defaultWorkDirectory();

        NodeFilesFinder nodeFilesFinder = new NodeFilesFinder();

        List<FileDescriptor> desc = nodeFilesFinder.find(workDirectory);

        List<FileDescriptor> cp = desc.stream().filter(d -> d.type() == CP).collect(toList());
        List<FileDescriptor> wal = desc.stream().filter(d -> d.type() == WAL).collect(toList());
        List<FileDescriptor> pageStore = desc.stream().filter(d -> d.type() == PAGE_STORE).collect(toList());
        List<FileDescriptor> index = desc.stream().filter(d -> d.type() == INDEX_STORE).collect(toList());
        List<FileDescriptor> nodeStart = desc.stream().filter(d -> d.type() == NODE_START).collect(toList());

        System.out.println("cp " + cp.size() + " wal " + wal.size() +
            " pageStore " + pageStore.size() + " index " + index.size() + " nodeStart " + nodeStart.size());
    }
}
