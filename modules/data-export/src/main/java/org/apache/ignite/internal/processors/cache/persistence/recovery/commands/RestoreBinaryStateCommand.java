package org.apache.ignite.internal.processors.cache.persistence.recovery.commands;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PageDeltaRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PartitionDestroyRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PartitionMetaStateRecord;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryImpl;
import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.Finder.Descriptor;
import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.NodeFilesFinder;
import org.apache.ignite.internal.util.GridMultiCollectionWrapper;
import org.apache.ignite.lang.IgniteBiTuple;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.processors.cache.persistence.recovery.finder.Finder.Type.CP;
import static org.apache.ignite.internal.processors.cache.persistence.recovery.finder.Finder.Type.WAL;

public class RestoreBinaryStateCommand implements Command {

    @Override public void execute(String... args) {
        NodeFilesFinder finder = new NodeFilesFinder();

        List<Descriptor> files = finder.find("root", CP, WAL);

        List<Descriptor> chps = files.stream().filter(desc -> desc.type() == CP).collect(toList());
        List<Descriptor> WALS = files.stream().filter(desc -> desc.type() == WAL).collect(toList());

        PageMemoryEx pageMemoryEx = createMemory();

        try (WALIterator it = null) {
            applyBinaryUpdates(it, pageMemoryEx);
        }
        catch (IgniteCheckedException e) {

        }

        try {
            finalizeCheckpointOnRecovery(0, null, null, pageMemoryEx);
        }
        catch (IgniteCheckedException e) {

        }
    }

    private void applyBinaryUpdates(WALIterator it, PageMemoryEx pageMem) throws IgniteCheckedException {
        boolean apply = true;

        int applied = 0;

        while (it.hasNextX()) {
            IgniteBiTuple<WALPointer, WALRecord> tup = it.nextX();

            WALRecord rec = tup.get2();

            switch (rec.type()) {
                case CHECKPOINT_RECORD:
                    CheckpointRecord cpRec = (CheckpointRecord)rec;

                    break;

                case PAGE_RECORD:
                    if (apply) {
                        PageSnapshot pageRec = (PageSnapshot)rec;

                        // Here we do not require tag check because we may be applying memory changes after
                        // several repetitive restarts and the same pages may have changed several times.
                        int grpId = pageRec.fullPageId().groupId();
                        long pageId = pageRec.fullPageId().pageId();

                        long page = pageMem.acquirePage(grpId, pageId, true);

                        try {
                            long pageAddr = pageMem.writeLock(grpId, pageId, page);

                            try {
                                PageUtils.putBytes(pageAddr, 0, pageRec.pageData());
                            }
                            finally {
                                pageMem.writeUnlock(grpId, pageId, page, null, true, true);
                            }
                        }
                        finally {
                            pageMem.releasePage(grpId, pageId, page);
                        }

                        applied++;
                    }

                    break;

                case PART_META_UPDATE_STATE:
                    PartitionMetaStateRecord metaStateRecord = (PartitionMetaStateRecord)rec;

                    break;

                case PARTITION_DESTROY:
                    PartitionDestroyRecord destroyRecord = (PartitionDestroyRecord)rec;

                    break;

                default:
                    if (apply && rec instanceof PageDeltaRecord) {
                        PageDeltaRecord r = (PageDeltaRecord)rec;

                        int grpId = r.groupId();

                        long pageId = r.pageId();

                        // Here we do not require tag check because we may be applying memory changes after
                        // several repetitive restarts and the same pages may have changed several times.
                        long page = pageMem.acquirePage(grpId, pageId, true);

                        try {
                            long pageAddr = pageMem.writeLock(grpId, pageId, page);

                            try {
                                r.applyDelta(pageMem, pageAddr);
                            }
                            finally {
                                pageMem.writeUnlock(grpId, pageId, page, null, true, true);
                            }
                        }
                        finally {
                            pageMem.releasePage(grpId, pageId, page);
                        }

                        applied++;
                    }
            }
        }
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void finalizeCheckpointOnRecovery(
        long cpTs,
        UUID cpId,
        WALPointer walPtr,
        PageMemoryEx pageMem
    ) throws IgniteCheckedException {
        assert cpTs != 0;

        GridMultiCollectionWrapper<FullPageId> pageIds = pageMem.beginCheckpoint();

        ByteBuffer tmpWriteBuf = ByteBuffer.allocateDirect(4096);

        tmpWriteBuf.order(ByteOrder.nativeOrder());

        // Identity stores set.
        Collection<PageStore> updStores = new HashSet<>();

        for (FullPageId pageId : pageIds) {
            tmpWriteBuf.rewind();

            Integer tag = pageMem.getForCheckpoint(pageId, tmpWriteBuf, null);

            if (tag != null) {
                tmpWriteBuf.rewind();

                // PageStore store = storeMgr.writeInternal(pageId.groupId(), pageId.pageId(), tmpWriteBuf, tag, true);

                tmpWriteBuf.rewind();

                updStores.add(null);
            }
        }

        for (PageStore updStore : updStores)
            updStore.sync();
    }

    private PageMemoryEx createMemory() {
        DataStorageConfiguration memCfg = new DataStorageConfiguration();

        DataRegionConfiguration cfg = new DataRegionConfiguration();

        cfg.setPersistenceEnabled(true);

        DirectMemoryProvider memProvider = new UnsafeMemoryProvider(null);

        DataRegionMetricsImpl memMetrics = new DataRegionMetricsImpl(cfg);

        return (PageMemoryEx)createPageMemory(memProvider, memCfg, cfg, memMetrics);

    }

    private PageMemory createPageMemory(
        DirectMemoryProvider memProvider,
        DataStorageConfiguration memCfg,
        DataRegionConfiguration plcCfg,
        DataRegionMetricsImpl memMetrics
    ) {
        memMetrics.persistenceEnabled(true);

        long cacheSize = plcCfg.getMaxSize();

        PageMemoryImpl pageMem = new PageMemoryImpl(
            memProvider,
            new long[] {cacheSize},
            null,
            memCfg.getPageSize(),
            (fullId, pageBuf, tag) -> {
                memMetrics.onPageWritten();

            },
            null,
            () -> true,
            memMetrics,
            PageMemoryImpl.ThrottlingPolicy.DISABLED,
            null
        );

        memMetrics.pageMemory(pageMem);

        return pageMem;
    }
}
