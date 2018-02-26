package org.apache.ignite.plugin.recovery.scan.elements;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPagePayload;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import sun.nio.ch.DirectBuffer;

public class FrameChainBuilder extends ScanAdapter {
    private final int pageSize = 4096;

    public static final int THRESHOLD = 4030;

    private final Map<Long, Frame> frames = new HashMap<>();

    private final List<Consumer<Frame>> frameConsumers = new LinkedList<>();

    public void addConsumer(Consumer<Frame> consumer) {
        frameConsumers.add(consumer);
    }

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

            onNextFrame(link, bytes, nextLink);
        }
    }

    public void onNextFrame(long link, byte[] bytes, long nextLink) {
        Frame waiter = frames.remove(link);

        Frame frame = new Frame(link, bytes, nextLink);

        if (frame.payload.length < THRESHOLD)
            frame.head = frame;

        if (waiter == null)
            frames.put(link, frame);
        else {
            waiter.next = frame;

            frame.head = waiter.head;
        }

        if (nextLink != 0) {
            Frame nextFrame = frames.remove(nextLink);

            if (nextFrame != null) {
                frame.next = nextFrame;

                if (frame.head != null) {
                    Frame tmp = frame.next;

                    while (true) {
                        if (tmp == null)
                            break;

                        tmp.head = frame.head;

                        if (tmp.nextLink == 0) {
                            chainDone(frame.head, true);

                            break;
                        }

                        tmp = tmp.next;
                    }
                }
            }
            else
                frames.put(nextLink, frame);
        }
        else {
            if (frame.head != null)
                chainDone(frame.head, true);
        }
    }

    private void chainDone(Frame head, boolean remove) {
        // assert head.nextLink != 0 && head.next != null;

        recursiveLen(head, remove);

        frameConsumers.forEach(c -> c.accept(head));
    }

    private int recursiveLen(Frame frame, boolean remove) {
        if (remove)
            frames.remove(frame.link);

        if (frame.next == null)
            return frame.len = frame.payload.length;

        frame.len += (frame.payload.length + recursiveLen(frame.next, remove));

        return frame.len;
    }

    @Override public void onComplete() {
        super.onComplete();

        if (!frames.isEmpty()) {
            for (Frame frame : frames.values())
                chainDone(frame, false);

            frames.clear();
        }
    }
}
