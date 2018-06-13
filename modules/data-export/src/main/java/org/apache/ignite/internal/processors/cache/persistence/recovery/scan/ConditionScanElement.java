package org.apache.ignite.internal.processors.cache.persistence.recovery.scan;

public interface ConditionScanElement extends ScanElement {
    boolean applicable();
}
