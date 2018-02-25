package org.apache.ignite.plugin.recovery.scan.elements;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Consumer;

public class DataPayloadExtractor extends ScanAdapter {
    private final FrameChainBuilder frameChainBuilder;

    public DataPayloadExtractor(Consumer<byte[]> consumerPayload) {
        frameChainBuilder = new FrameChainBuilder(
            (head) -> {
                ByteBuffer buf = ByteBuffer.allocate(head.len);

                buf.order(ByteOrder.nativeOrder());
                while (true) {
                    buf.put(head.payload);

                    head = head.next;

                    if (head == null)
                        break;
                }

                consumerPayload.accept(buf.array());
            }
        );
    }

    @Override public void onNextPage(ByteBuffer buf) {
        frameChainBuilder.onNextPage(buf);
    }

    @Override public void onComplete() {
        frameChainBuilder.onComplete();
    }
}
