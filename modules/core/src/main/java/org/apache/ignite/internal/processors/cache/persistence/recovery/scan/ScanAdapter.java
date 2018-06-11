package org.apache.ignite.internal.processors.cache.persistence.recovery.scan;

public abstract class ScanAdapter implements ScanElement {

    @Override public void onComplete() {
        // No-op.
    }
}
