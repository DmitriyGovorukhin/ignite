package org.apache.ignite.internal.processors.cache.persistence.recovery.read.payload;

import org.apache.ignite.internal.processors.cache.persistence.recovery.model.Payload;
import org.apache.ignite.internal.processors.cache.persistence.recovery.read.ReadHandler;

public abstract class PayloadReadHandler implements ReadHandler<Payload> {

    @Override public void onComplete() {
        // No-op.
    }
}
