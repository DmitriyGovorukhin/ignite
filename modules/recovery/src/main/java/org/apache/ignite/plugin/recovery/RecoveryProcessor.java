package org.apache.ignite.plugin.recovery;

import java.io.File;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridKernalContextImpl;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.persistence.file.AsyncFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.PureJavaCrc32;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.ignite.plugin.recovery.RecoveryUtils.filePageStoreManager;

public class RecoveryProcessor {
    private final RecoveryConfiguration recoveryConfiguration;

    private final GridKernalContext ctx;

    private final FileIOFactory ioFactory;

    public RecoveryProcessor(
        RecoveryConfiguration cfg,
        GridKernalContext ctx
    ) {
        this.recoveryConfiguration = cfg;
        this.ctx = ctx;
        this.ioFactory = new AsyncFileIOFactory();
    }

    public IgniteFuture<?> restoreDataBase() {
        System.err.println("restore database");

        return new IgniteFinishedFutureImpl<>();
    }

    public IgniteFuture<?> verifyWal() {
        return new IgniteFinishedFutureImpl<>();
    }

    public IgniteFuture<?> verifyCacheConfigurations() {
        return new IgniteFinishedFutureImpl<>();
    }

    public IgniteFuture<List<FullPageId>> verifyPartitions() {
        FilePageStoreManager pageStoreManager = filePageStoreManager(ctx);

        List<CacheConfiguration> cfgs = new LinkedList<>();

        try {
            Map<String, StoredCacheData> cacheStores = pageStoreManager.readCacheConfigurations();

            for (StoredCacheData sc : cacheStores.values())
                cfgs.add(sc.config());
        }
        catch (IgniteCheckedException e) {
            return new IgniteFinishedFutureImpl<>(e);
        }

        int pageSize = ctx.config().getDataStorageConfiguration().getPageSize();

        ByteBuffer buf = ByteBuffer.allocate(pageSize);

        buf.order(ByteOrder.nativeOrder());

        List<FullPageId> corruptedPages = new LinkedList<>();

        try {
            for (CacheConfiguration cacheCfg : cfgs) {
                boolean isShared = cacheCfg.getGroupName() != null;

                String cacheName = isShared ? cacheCfg.getGroupName() : cacheCfg.getName();

                int partitions = cacheCfg.getAffinity().partitions();

                for (int id = 0; id < partitions; id++) {
                    Path path = pageStoreManager.getPath(isShared, cacheName, id);

                    File partition = path.toFile();

                    if (partition.exists()) {
                        FileIO file = ioFactory.create(partition, READ);

                        long size = file.size();

                        if (size % pageSize != 0)
                            System.out.println("WARNING! size:" + size + " pageSize:" + pageSize);

                        for (long pos = pageSize; pos < size; pos += pageSize) {
                            file.read(buf, pos);

                            buf.rewind();

                            int crcSaved = PageIO.getCrc(buf);

                            PageIO.setCrc(buf, 0);

                            if (crcSaved == -42)
                                System.out.println("-42");

                            int currCrc = PureJavaCrc32.calcCrc32(buf, pageSize);

                            if (currCrc != crcSaved)
                                corruptedPages.add(
                                    new FullPageId(
                                        PageIO.getPageId(buf),
                                        CU.cacheId(cacheName)
                                    )
                                );

                            buf.rewind();
                        }
                    }
                }
            }

        }
        catch (IOException e) {
            return new IgniteFinishedFutureImpl<>(e);
        }

        return new IgniteFinishedFutureImpl<>(corruptedPages);
    }
}
