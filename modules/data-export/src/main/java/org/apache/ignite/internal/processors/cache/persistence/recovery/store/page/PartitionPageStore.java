package org.apache.ignite.internal.processors.cache.persistence.recovery.store.page;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.filedescriptors.PageStoreDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.recovery.store.Store;

public class PartitionPageStore implements Store<ByteBuffer> {

    private final FileIO fileIO;

    private final int pageSize;

    private final int headerSize;

    private final long length;

    private final PageStoreDescriptor desc;

    public PartitionPageStore(PageStoreDescriptor desc, FileIO fileIO) {
        this.desc = desc;
        this.fileIO = fileIO;

        length = desc.size();
        headerSize = desc.pageSize();
        pageSize = desc.pageSize();
    }

    @Override public void read(long pageId, ByteBuffer buf) throws IOException {
        fileIO.read(buf, pageOffset(pageId));
    }

    private PageIterator iterator(long pageLowBound, long pageHighBound) {
        long highBound = 0;

        if (pageHighBound < length / pageSize)
            highBound = pageHighBound;

        long lowBound = 0;

        if (lowBound > 0)
            lowBound = pageLowBound;

        return new PageStoreIterator(headerSize + lowBound, highBound);
    }

    @Override public PageIterator iterator() {
        return iterator(0, Long.MAX_VALUE);
    }

    @Override public PageStoreDescriptor descriptor() {
        return desc;
    }

    private long pageOffset(long pageId) {
        return (long)PageIdUtils.pageIndex(pageId) * pageSize + headerSize;
    }

    /**
     *
     */
    private class PageStoreIterator implements PageIterator {
        /** */
        private final long highBound;

        /** */
        private long offset;

        private PageStoreIterator(long lowBound, long highBound) {
            this.highBound = highBound;

            offset = lowBound;
        }

        @Override public boolean hasNext() {

            return offset <= (length - pageSize - highBound);
        }

        @Override public void next(ByteBuffer buf) throws IgniteCheckedException {
            try {
                fileIO.read(buf, offset);

                offset += pageSize;
            }
            catch (IOException e) {
                throw new IgniteCheckedException(e);
            }
        }
    }
}
