package org.apache.ignite.internal.processors.cache.persistence.recovery.stores;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.recovery.PageIterator;
import org.apache.ignite.internal.processors.cache.persistence.recovery.PageStore;
import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.FilePageStoreDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.PageStoreDescriptor;

public class RecoveryPageStore implements PageStore {

    private final FileIO fileIO;

    private final int pageSize;

    private final int headerSize;

    private final long length;

    private final PageStoreDescriptor desc;

    public RecoveryPageStore(FilePageStoreDescriptor desc) {
        this.desc = desc;

        fileIO = desc.fileIO();
        length = desc.size();
        headerSize = desc.pageSize();
        pageSize = desc.pageSize();
    }

    @Override public int readPage(long pageId, ByteBuffer buf) throws IOException {
        return fileIO.read(buf, pageOffset(pageId));
    }

    @Override public PageIterator iterator(long pageLowBound, long pageHighBound) {
        long highBound = 0;

        if (pageHighBound < length / pageSize)
            highBound = pageHighBound;

        long lowBound = 0;

        if (lowBound > 0)
            lowBound = pageLowBound;

        long finalLowBound = lowBound;
        long finalHighBound = highBound;

        return new PageIterator() {
            private long offset = headerSize + finalLowBound;

            @Override public boolean hasNext() {

                return offset <= (length - pageSize - finalHighBound);
            }

            @Override public int next(ByteBuffer buf) throws IOException {
                int read = fileIO.read(buf, offset);

                offset += pageSize;

                return read;
            }
        };
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
}
