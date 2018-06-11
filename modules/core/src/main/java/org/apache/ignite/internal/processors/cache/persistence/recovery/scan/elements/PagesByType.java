package org.apache.ignite.internal.processors.cache.persistence.recovery.scan.elements;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.PageStoreDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.recovery.scan.ScanAdapter;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;

public class PagesByType extends ScanAdapter {
    private final Map<Integer, Integer> pagesByType = new HashMap<>();

    public PagesByType(PageStoreDescriptor descriptor) {
        super(descriptor);
    }

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
