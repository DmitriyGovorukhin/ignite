package org.apache.ignite.plugin.recovery.scan.elements;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

public class KeyValueExtractor extends ScanAdapter {

    private final FrameChainBuilder frameChainBuilder;

    public KeyValueExtractor(Consumer<KeyValue> payloadConsumer) {
        frameChainBuilder = new FrameChainBuilder(
            frame -> payloadConsumer.accept(frameToKeyValue(frame))
        );
    }

    @Override public void onNextPage(ByteBuffer buf) {
        frameChainBuilder.onNextPage(buf);
    }

    @Override public void onComplete() {
        frameChainBuilder.onComplete();
    }

    private KeyValue frameToKeyValue(Frame head) {
        FrameTransformer frameTransformer = new FrameTransformer();

        return frameTransformer.toKeyValue(head);
    }
}
