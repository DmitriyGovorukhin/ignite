package org.apache.ignite.internal.processors.cache.persistence.recovery.read.page.handlers.extractor;

public interface FrameTransformer<T> {
    T map(Frame head);
}
