package org.apache.ignite.plugin.recovery;

import java.io.File;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridKernalContextImpl;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
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

public class RecoveryProcessor {
    private final RecoveryConfiguration recoveryConfiguration;

    private final GridKernalContext ctx;

    private final List<CacheConfiguration> cfgs = new ArrayList<>();

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

    public IgniteFuture<?> verifyPartitions() {
        FilePageStoreManager pageStoreManager = filePageStoreManager(ctx);

        int pageSize = ctx.config().getDataStorageConfiguration().getPageSize();

        ByteBuffer buf = ByteBuffer.allocate(pageSize);

        buf.order(ByteOrder.nativeOrder());

        try {
            for (CacheConfiguration cacheCfg : cfgs) {
                boolean isShared = cacheCfg.getGroupName() != null;

                String cacheName = isShared ? cacheCfg.getGroupName() : cacheCfg.getName();

                int partitions = cacheCfg.getAffinity().partitions();

                for (int id = 0; id < partitions; id++) {
                    Path path = pageStoreManager.getPath(isShared, cacheName, id);

                    File partition = path.toFile();

                    if (partition.exists()) {
                        FileIO file = ioFactory.create(partition, READ, WRITE);

                        long size = file.size();

                        for (long pos = 0; pos < size; pos += size) {
                            file.read(buf, pos);

                            buf.rewind();

                            int crcSaved = PageIO.getCrc(buf);

                            PageIO.setCrc(buf, 0);

                            int currCrc = PureJavaCrc32.calcCrc32(buf, pageSize);

                            if (currCrc != crcSaved)
                                System.out.println("CRC");

                            buf.rewind();
                        }
                    }
                }
            }

        }
        catch (IOException e) {
            return new IgniteFinishedFutureImpl<>(e);
        }

        return new IgniteFinishedFutureImpl<>();
    }

    private static FilePageStoreManager filePageStoreManager(GridKernalContext ctx) {
        return (FilePageStoreManager)ctx.cache().context().pageStore();
    }
}
