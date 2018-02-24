package org.apache.ignite.plugin.recovery.scan.elements;

import org.apache.ignite.plugin.recovery.scan.ScanElement;

public abstract class ScanAdapter implements ScanElement {

    @Override public void onComplete() {
        // No-op.
    }
}
