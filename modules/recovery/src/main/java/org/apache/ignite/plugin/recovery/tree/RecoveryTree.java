package org.apache.ignite.plugin.recovery.tree;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheSearchRow;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIO;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.plugin.recovery.memory.PageMemory;

import static org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryImpl.PAGE_OVERHEAD;

public class RecoveryTree implements Tree<CacheSearchRow, CacheDataRow> {

    private final PageMemory mem;

    private final int grpId;

    private final int partId;

    private long metaPageId;

    public RecoveryTree(PageMemory mem, int grpId, int partId) throws IgniteCheckedException {
        this.mem = mem;
        this.grpId = grpId;
        this.partId = partId;

        init();
    }

    private void init() throws IgniteCheckedException {
        long metaPageId = PageIdUtils.pageId(partId, PageIdAllocator.FLAG_DATA, 0);

        long metaPageAbsPtr = mem.acquirePage(grpId, metaPageId);

        try {
            long ptr = pageOffSet(metaPageAbsPtr);

            PagePartitionMetaIO io = PageIO.getPageIO(ptr);

            metaPageId = io.getTreeRoot(ptr);
        }
        finally {
            mem.releasePage(grpId, partId, metaPageAbsPtr);
        }
    }

    @Override public CacheDataRow find(CacheSearchRow key) throws IgniteCheckedException {
        return null;
    }

    @Override public GridCursor<CacheDataRow> find(
        CacheSearchRow lower,
        CacheSearchRow upper
    ) throws IgniteCheckedException {
        return null;
    }

    @Override public long size() throws IgniteCheckedException {
        return 0;
    }

    private static long pageOffSet(long abs) {
        return abs + PAGE_OVERHEAD;
    }
}
