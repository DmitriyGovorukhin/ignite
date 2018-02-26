package org.apache.ignite.plugin.recovery;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPagePayload;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.plugin.recovery.scan.PageStoreScanner;
import org.apache.ignite.plugin.recovery.scan.elements.PageCounter;
import org.apache.ignite.plugin.recovery.scan.elements.ScanAdapter;
import org.apache.ignite.plugin.recovery.store.FilePageStoreFactory;
import org.apache.ignite.plugin.recovery.store.PageStore;
import org.apache.ignite.plugin.recovery.store.PageStoreFactory;
import sun.nio.ch.DirectBuffer;

public class RecoveryProcessor {

    private final IgniteLogger log;

    private final GridKernalContext ctx;

    private final RecoveryConfiguration recoveryConfiguration;

    private final FileIOFactory fileIOFactory;

    private final PageStoreFactory pageStoreFactory;

    public RecoveryProcessor(
        RecoveryConfiguration cfg,
        GridKernalContext ctx
    ) {
        this.log = ctx.log(RecoveryProcessor.class);
        this.ctx = ctx;
        this.recoveryConfiguration = cfg;
        this.fileIOFactory = ctx.config().getDataStorageConfiguration().getFileIOFactory();
        this.pageStoreFactory = new FilePageStoreFactory(fileIOFactory);
    }

    public IgniteFuture<?> clonePartitionWithDataClearing(
        long snapshotId,
        Map<String, Set<Integer>> parts,
        File opt
    ) {
        Object consistentId = ctx.discovery().localNode().consistentId();

        DataStorageConfiguration dsCfg = ctx.config().getDataStorageConfiguration();

        int pageSize = dsCfg.getPageSize();

        String storePath = dsCfg.getStoragePath();

        File storeFile = new File(storePath, consistentId.toString());

        try {
            for (Map.Entry<String, Set<Integer>> entry : parts.entrySet()) {
                String cacheOrGroupName = entry.getKey();

                File cacheOrGroupDir = new File(storeFile, "cache-" + cacheOrGroupName);
                File cacheOrGroupDirClone = new File(opt, "cache-" + cacheOrGroupName);

                for (Integer partId : entry.getValue()) {
                    String partIdStr = String.valueOf(partId);

                    File partCloneFile = new File(cacheOrGroupDirClone, "part-" + partIdStr + ".bin");

                    if (!cacheOrGroupDirClone.exists())
                        cacheOrGroupDirClone.mkdirs();
                    else
                        cacheOrGroupDirClone.delete();

                    File partFile = new File(cacheOrGroupDir, "part-" + partIdStr + ".bin");

                    PageStore partStore = pageStoreFactory.createStore(partFile);

                    PageStoreScanner scanner = PageStoreScanner.create(partStore);

                    ByteBuffer writeBuffer = ByteBuffer.allocate(pageSize);

                    PageCounter pageCounter = new PageCounter();

                    scanner.addHandler(pageCounter);

                    scanner.addHandler(new ScanAdapter() {
                        private final FileIO fileIO = fileIOFactory.create(
                            partCloneFile, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);

                        @Override public void onNextPage(ByteBuffer buf) {
                            if (PageIO.getType(buf) != PageIO.T_DATA) {
                                try {
                                    fileIO.write(buf);
                                }
                                catch (IOException e) {
                                    log.error("Error write page", e);
                                }

                                return;
                            }

                            writeBuffer.put(buf);

                            long ptr = ((DirectBuffer)buf).address();

                            DataPageIO io = DataPageIO.VERSIONS.forPage(ptr);

                            long pageId = PageIO.getPageId(ptr);

                            int items = io.getDirectCount(ptr);

                            for (int i = 0; i < items; i++) {
                                DataPagePayload payload = io.readPayload(ptr, i, pageSize);

                                writeBuffer.position(payload.offset());

                                writeBuffer.put(new byte[payload.payloadSize()]);
                            }

                            writeBuffer.flip();

                            try {
                                fileIO.write(writeBuffer);
                            }
                            catch (IOException e) {
                                log.error("Error write page", e);
                            }

                            writeBuffer.clear();
                        }

                        @Override public void onComplete() {
                            try {
                                fileIO.force();

                                fileIO.close();
                            }
                            catch (IOException e) {
                                log.error("Error close file page", e);
                            }
                        }
                    });

                    scanner.scan();

                    System.out.println("Pages write " + pageCounter.pages() + ", page size " + pageSize);
                }
            }
        }
        catch (IOException e) {
            return new IgniteFinishedFutureImpl<>(e);
        }

        return new IgniteFinishedFutureImpl<>();
    }
}
