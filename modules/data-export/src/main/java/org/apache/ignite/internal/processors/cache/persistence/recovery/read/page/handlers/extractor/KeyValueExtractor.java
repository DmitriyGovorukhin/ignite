package org.apache.ignite.internal.processors.cache.persistence.recovery.read.page.handlers.extractor;

import java.util.function.Consumer;
import org.apache.ignite.internal.processors.cache.persistence.recovery.model.KeyValue;

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
