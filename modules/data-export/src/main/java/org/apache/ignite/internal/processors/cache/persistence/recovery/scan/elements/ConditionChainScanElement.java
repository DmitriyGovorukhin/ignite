package org.apache.ignite.internal.processors.cache.persistence.recovery.scan.elements;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.StoreDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.recovery.scan.ConditionScanElement;
import org.apache.ignite.internal.processors.cache.persistence.recovery.scan.ScanAdapter;

public class ConditionChainScanElement<A extends ScanAdapter, B extends ConditionScanElement> extends ScanAdapter {

    private final A next;
    private final B condition;

    public ConditionChainScanElement(StoreDescriptor descriptor, A next, B condition) {
        super(descriptor);
        this.next = next;
        this.condition = condition;
    }

    @Override public void onNextPage(ByteBuffer buf) {
        condition.onNextPage(buf);

        if (condition.applicable())
            next.onNextPage(buf);
    }
}
