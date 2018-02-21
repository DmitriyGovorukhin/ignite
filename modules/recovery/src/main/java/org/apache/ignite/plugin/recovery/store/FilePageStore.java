package org.apache.ignite.plugin.recovery.store;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.StandardOpenOption;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;

public class FilePageStore implements PageStore {

    private final File file;

    private final FileIO fileIO;

    private final int pageSize;

    private final int headerSize;

    private final long length;

    private final boolean corrupted;

    public FilePageStore(File file, FileIOFactory factory) throws IOException {
        this.file = file;
        this.fileIO = factory.create(file, StandardOpenOption.READ);
        this.length = file.length();
        this.headerSize = 0;
        this.pageSize = 0;
        this.corrupted = (length - headerSize) % pageSize != 0;
    }

    @Override public int pageSize() {
        return pageSize;
    }

    @Override public void readPage(long pageId, ByteBuffer buf) throws IOException {
        fileIO.read(buf, pageOffset(pageId));
    }

    @Override public PageIterator iterator() {
        return new PageIterator() {
            private int offset = headerSize;

            @Override public boolean hasNext() {
                return offset <= (length - pageSize);
            }

            @Override public void next(ByteBuffer buf) throws IOException {
                fileIO.read(buf, offset);

                offset += pageSize;
            }
        };
    }

    private long pageOffset(long pageId) {
        return (long)PageIdUtils.pageIndex(pageId) * pageSize + headerSize;
    }
}
