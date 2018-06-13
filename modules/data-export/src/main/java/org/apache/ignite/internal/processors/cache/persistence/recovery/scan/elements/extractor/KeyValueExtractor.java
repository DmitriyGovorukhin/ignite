package org.apache.ignite.internal.processors.cache.persistence.recovery.scan.elements.extractor;

import java.util.function.Consumer;

public class KeyValueExtractor {

    public KeyValueExtractor(
        Consumer<KeyValue> keyValueConsumer,
        FrameChainBuilder frameChainBuilder
    ) {
        frameChainBuilder.addConsumer(frame -> keyValueConsumer.accept(frameToKeyValue(frame)));
    }

    private KeyValue frameToKeyValue(Frame head) {
        FrameTransformer frameTransformer = new FrameTransformer();

        return frameTransformer.toKeyValue(head);
    }
}
