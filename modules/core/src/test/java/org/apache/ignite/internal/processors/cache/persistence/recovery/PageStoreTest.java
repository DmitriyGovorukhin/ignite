package org.apache.ignite.internal.processors.cache.persistence.recovery;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.persistence.AllocatedPageTracker;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FileVersionCheckingFactory;
import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.FilePageStoreDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.FilePageStoreFinder;
import org.apache.ignite.internal.processors.cache.persistence.recovery.stores.RecoveryPageStore;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;

public class PageStoreTest extends GridCommonAbstractTest {
    private static final AllocatedPageTracker NOOP_TRACKER = (delta) -> {
    };

    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        U.delete(Paths.get(U.defaultWorkDirectory()));
    }

    public void test() throws IgniteCheckedException, IOException {
        String workDir = U.defaultWorkDirectory();

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        dsCfg.setPageSize(DataStorageConfiguration.DFLT_PAGE_SIZE);

        FilePageStoreFactory f = new FileVersionCheckingFactory(dsCfg.getFileIOFactory(), dsCfg);

        File file = new File(workDir, "part-0.bin");

        FilePageStore store = f.createPageStore(PageMemory.FLAG_DATA, file, NOOP_TRACKER);

        int pageSize = dsCfg.getPageSize();

        long ptr = GridUnsafe.allocateMemory(pageSize);

        ByteBuffer buf0 = GridUnsafe.wrapPointer(ptr, pageSize);

        DataPageIO dataPageIO = DataPageIO.VERSIONS.latest();

        int pages = 1000;

        try {
            for (int i = 0; i < pages; i++) {
                long pageId = store.allocatePage();

                dataPageIO.initNewPage(ptr, pageId, pageSize);

                store.write(pageId, buf0, 0, true);

                buf0.clear();
            }
        }
        finally {
            store.sync();

            GridUnsafe.freeBuffer(buf0);
        }

        FilePageStoreFinder storeFinder = new FilePageStoreFinder();

        List<FilePageStoreDescriptor> stores = storeFinder.findStores(U.defaultWorkDirectory());

        RecoveryPageStore recoveryPageStore = new RecoveryPageStore(stores.get(0));

        PageIterator it = recoveryPageStore.iterator();

        ByteBuffer buf1 = GridUnsafe.allocateBuffer(pageSize);

        int dataPages = 0;

        try {
            while (it.hasNext()) {
                int read = it.next(buf1);

                assertTrue(read > 0);

                buf1.rewind();

                int type = PageIO.getType(buf1);

                if (type == PageIO.T_DATA)
                    dataPages++;
            }
        }
        finally {
            GridUnsafe.freeBuffer(buf1);
        }

        Assert.assertEquals(pages, dataPages);
    }
}
