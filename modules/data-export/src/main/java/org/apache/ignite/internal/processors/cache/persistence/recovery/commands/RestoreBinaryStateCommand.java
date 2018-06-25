package org.apache.ignite.internal.processors.cache.persistence.recovery.commands;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
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
import org.apache.ignite.internal.util.GridMultiCollectionWrapper;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.lang.IgniteBiTuple;

import static org.apache.ignite.internal.pagemem.PageIdUtils.partId;

public class RestoreBinaryStateCommand implements Command {

    @Override public void execute(String... args) {
        String WALDir = args[0];
        String CPFDir = args[1];
        String pageStoreDir = args[2];

        try {
            PageMemoryEx pageMemoryEx = createMemory();

            try (WALIterator it = null) {
                applyBinaryUpdates(it, pageMemoryEx);
            }

            Map<Integer, List<PageStore>> pageStores = initPageStores(pageStoreDir);

            writeDirtyPages(pageMemoryEx, (tup) -> {
                FullPageId fullPageId = tup.get1();

                ByteBuffer buf = tup.get2();

                int tag = tup.get3();

                int partId = partId(fullPageId.pageId());

                List<PageStore> stores = pageStores.get(fullPageId.groupId());

                PageStore pageStore = stores.get(partId);

                try {
                    pageStore.write(partId, buf, tag, true);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }

                return pageStore;
            });
        }
        catch (IgniteCheckedException e) {

        }

    }

    private void applyBinaryUpdates(WALIterator it, PageMemoryEx pageMem) throws IgniteCheckedException {
        boolean apply = true;

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
                    }
            }
        }
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void writeDirtyPages(
        PageMemoryEx pageMem,
        Function<T3<FullPageId, ByteBuffer, Integer>, PageStore> onPageWrite
    ) throws IgniteCheckedException {

        GridMultiCollectionWrapper<FullPageId> pageIds = pageMem.beginCheckpoint();

        ByteBuffer tmpWriteBuf = ByteBuffer.allocateDirect(pageMem.pageSize());

        tmpWriteBuf.order(ByteOrder.nativeOrder());

        Collection<PageStore> updStores = new HashSet<>();

        for (FullPageId pageId : pageIds) {
            tmpWriteBuf.rewind();

            Integer tag = pageMem.getForCheckpoint(pageId, tmpWriteBuf, null);

            if (tag != null) {
                tmpWriteBuf.rewind();

                PageStore store = onPageWrite.apply(new T3<>(pageId, tmpWriteBuf, tag));

                tmpWriteBuf.rewind();

                updStores.add(store);
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

    private Map<Integer, List<PageStore>> initPageStores(String pageStoreDir) {
        return new HashMap<>();
    }
}
