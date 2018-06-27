package org.apache.ignite.internal.processors.cache.persistence.recovery.read.page.handlers.extractor;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Consumer;
import org.apache.ignite.internal.processors.cache.persistence.recovery.model.Payload;

public class DataPayloadTransformer implements FrameTransformer<Payload> {

    @Override public Payload map(Frame head) {
        ByteBuffer buf = ByteBuffer.allocate(head.len);

        buf.order(ByteOrder.nativeOrder());

        Frame frame = head;

        do {
            buf.put(frame.payload);

            frame = frame.next;

        }
        while (frame != null);

        return new Payload(buf.array());
    }
}
