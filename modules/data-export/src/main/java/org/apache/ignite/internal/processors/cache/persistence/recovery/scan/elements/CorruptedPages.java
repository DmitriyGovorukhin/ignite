package org.apache.ignite.internal.processors.cache.persistence.recovery.scan.elements;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.PageStoreDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.recovery.scan.ScanAdapter;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.PureJavaCrc32;

public class CorruptedPages extends ScanAdapter {

    private final Set<Long> corruptedPages = new HashSet<>();

    public CorruptedPages(PageStoreDescriptor descriptor) {
        super(descriptor);
    }

    @Override public void onNextPage(ByteBuffer buf) {
        int pageSize = buf.capacity();

        long pageId = PageIO.getPageId(buf);

        int crc = PageIO.getCrc(buf);

        PageIO.setCrc(buf, 0);

        int currCrc = PureJavaCrc32.calcCrc32(buf, pageSize);

        if (crc != currCrc)
            corruptedPages.add(pageId);
    }

    public Set<Long> corruptedPages() {
        return Collections.unmodifiableSet(corruptedPages);
    }
}
