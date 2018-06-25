package org.apache.ignite.internal.processors.cache.persistence.recovery.commands;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.regex.Matcher;
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
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.CheckpointStatus;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointEntryType;
import org.apache.ignite.internal.processors.cache.persistence.file.AsyncFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryImpl;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.util.GridMultiCollectionWrapper;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.lang.IgniteBiTuple;

import static java.nio.file.StandardOpenOption.READ;
import static org.apache.ignite.internal.pagemem.PageIdUtils.partId;
import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.CP_FILE_NAME_PATTERN;

public class RestoreBinaryStateCommand implements Command {

    private final FileIOFactory ioFactory = new AsyncFileIOFactory();

    @Override public void execute(String... args) {
        String walDir = args[0];
        String cpDir = args[1];
        String pageStoreDir = args[2];

        try {
            //1. Read checkpoint status.
            CheckpointStatus checkpointStatus = readCheckpointStatus(cpDir);

            //2. Check need restore or not.
            if (!checkpointStatus.needRestoreMemory())
                return;

            //3. Prepare memory for restore .
            PageMemoryEx pageMemoryEx = createMemory();

            IgniteWalIteratorFactory iteratorFactory = new IgniteWalIteratorFactory();

            //4. Restore page snapshot and binary delta.
            try (WALIterator it = iteratorFactory.iterator(walDir)) {
                applyBinaryUpdates(it, pageMemoryEx);
            }

            //5. Prepare page stores for write.
            Map<Integer, List<PageStore>> pageStores = initializePageStores(pageStoreDir);

            //6. Write all in-memory update to page store.
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

            finishCheckpoint(checkpointStatus);
        }
        catch (IgniteCheckedException e) {
            e.printStackTrace();
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

    private Map<Integer, List<PageStore>> initializePageStores(String pageStoreDir) {
        return new HashMap<>();
    }

    private CheckpointStatus readCheckpointStatus(String cpDir) throws IgniteCheckedException {
        long lastStartTs = 0;
        long lastEndTs = 0;

        UUID startId = CheckpointStatus.NULL_UUID;
        UUID endId = CheckpointStatus.NULL_UUID;

        File startFile = null;
        File endFile = null;

        WALPointer startPtr = CheckpointStatus.NULL_PTR;
        WALPointer endPtr = CheckpointStatus.NULL_PTR;

        File dir = new File(cpDir);

        File[] files = dir.listFiles();

        for (File file : files) {
            Matcher matcher = CP_FILE_NAME_PATTERN.matcher(file.getName());

            if (matcher.matches()) {
                long ts = Long.parseLong(matcher.group(1));
                UUID id = UUID.fromString(matcher.group(2));
                CheckpointEntryType type = CheckpointEntryType.valueOf(matcher.group(3));

                if (type == CheckpointEntryType.START && ts > lastStartTs) {
                    lastStartTs = ts;
                    startId = id;
                    startFile = file;
                }
                else if (type == CheckpointEntryType.END && ts > lastEndTs) {
                    lastEndTs = ts;
                    endId = id;
                    endFile = file;
                }
            }
        }

        ByteBuffer buf = ByteBuffer.allocate(20);
        buf.order(ByteOrder.nativeOrder());

        if (startFile != null)
            startPtr = readPointer(startFile, buf);

        if (endFile != null)
            endPtr = readPointer(endFile, buf);

        return new CheckpointStatus(lastStartTs, startId, startPtr, endId, endPtr);
    }

    private WALPointer readPointer(File cpMarkerFile, ByteBuffer buf) throws IgniteCheckedException {
        buf.position(0);

        try (FileIO io = ioFactory.create(cpMarkerFile, READ)) {
            io.read(buf);

            buf.flip();

            return new FileWALPointer(buf.getLong(), buf.getInt(), buf.getInt());
        }
        catch (IOException e) {
            throw new IgniteCheckedException(
                "Failed to read checkpoint pointer from marker file: " + cpMarkerFile.getAbsolutePath(), e);
        }
    }

    private void finishCheckpoint(CheckpointStatus checkpointStatus) {

    }
}
