package org.apache.ignite.plugin.recovery.scan.elements.payloadextractor;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPagePayload;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.plugin.recovery.scan.elements.ScanAdapter;
import sun.nio.ch.DirectBuffer;

public class DataPayloadExtractor extends ScanAdapter {
    private final int pageSize = 4096;

    private final FrameChainBuilder frameChainBuilder = new FrameChainBuilder();

    private final Set<byte[]> payloadsSet = new HashSet<>();

    @Override public void onNextPage(ByteBuffer buf) {
        if (PageIO.getType(buf) != PageIO.T_DATA)
            return;

        long ptr = ((DirectBuffer)buf).address();

        DataPageIO io = DataPageIO.VERSIONS.forPage(ptr);

        long pageId = PageIO.getPageId(ptr);

        int items = io.getDirectCount(ptr);

        for (int i = 0; i < items; i++) {
            DataPagePayload payload = io.readPayload(ptr, i, pageSize);

            long payloadPtr = ptr + payload.offset();

            long nextLink = payload.nextLink();

            long link = PageIdUtils.link(pageId, i);

            byte[] bytes = PageUtils.getBytes(payloadPtr, 0, payload.payloadSize());

            frameChainBuilder.onNextFrame(link, bytes, nextLink);
        }
    }

    @Override public void onComplete() {
        for (Frame frame : frameChainBuilder.completelyChain()) {
            Frame head = frame;

            ByteBuffer buf = ByteBuffer.allocate(head.len);

            while (true) {
                buf.put(head.payload);

                head = head.next;

                if (head == null)
                    break;
            }

            payloadsSet.add(buf.array());
        }
    }

    public Set<Frame> frameSet() {
        return new HashSet<>(frameChainBuilder.completelyChain());
    }

    public Set<byte[]> payLoadSet() {
        return new HashSet<>(payloadsSet);
    }
}
