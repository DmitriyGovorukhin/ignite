package org.apache.ignite.internal.processors.cache.persistence.recovery.store.page;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.processors.cache.persistence.recovery.store.StoreIterator;

public interface PageIterator extends StoreIterator<ByteBuffer> {
}
