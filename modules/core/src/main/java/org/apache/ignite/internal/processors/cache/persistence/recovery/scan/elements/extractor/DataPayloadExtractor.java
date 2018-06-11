package org.apache.ignite.internal.processors.cache.persistence.recovery.scan.elements.extractor;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Consumer;

public class DataPayloadExtractor {

    public DataPayloadExtractor(
        Consumer<byte[]> consumerPayload,
        FrameChainBuilder frameChainBuilder
    ) {
        frameChainBuilder.addConsumer((frame) -> {
            ByteBuffer buf = ByteBuffer.allocate(frame.len);

            buf.order(ByteOrder.nativeOrder());
            while (true) {
                buf.put(frame.payload);

                frame = frame.next;

                if (frame == null)
                    break;
            }

            consumerPayload.accept(buf.array());
        });
    }
}
