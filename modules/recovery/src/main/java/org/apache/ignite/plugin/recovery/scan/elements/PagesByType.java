package org.apache.ignite.plugin.recovery.scan.elements;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.plugin.recovery.scan.ScanElement;

public class PagesByType implements ScanElement {
    private final Map<Integer, Integer> pagesByType = new HashMap<>();

    @Override public void onNextPage(ByteBuffer buf) {
        int type = PageIO.getType(buf);

        Integer cnt = pagesByType.get(type);

        if (cnt == null)
            pagesByType.put(type, 1);
        else
            pagesByType.put(type, ++cnt);
    }

    public Map<Integer, Integer> pagesByType() {
        return new HashMap<>(pagesByType);
    }
}
