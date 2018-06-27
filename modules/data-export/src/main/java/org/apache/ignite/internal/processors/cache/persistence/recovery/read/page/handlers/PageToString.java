package org.apache.ignite.internal.processors.cache.persistence.recovery.read.page.handlers;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.filedescriptors.PageStoreDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.recovery.read.page.PageReadHandler;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import sun.nio.ch.DirectBuffer;

public class PageToString extends PageReadHandler {
    private final Set<Integer> printPagesByType;

    private final Map<Integer, List<String>> stringPages = new HashMap<>();

    public PageToString(PageStoreDescriptor descriptor, int... types) {
        super(descriptor);

        printPagesByType = new HashSet<>();

        for (int i = 0; i < types.length; i++)
            printPagesByType.add(types[i]);
    }

    @Override public void onNextRead(ByteBuffer buf) {
        int type = PageIO.getType(buf);

        if (printPagesByType.contains(type)) {
            long ptr = ((DirectBuffer)buf).address();

            try {
                List<String> pagesString = stringPages.getOrDefault(type, new ArrayList<>());

                pagesString.add(PageIO.printPage(ptr, pageSize));

                stringPages.put(type, pagesString);
            }
            catch (IgniteCheckedException e) {
                e.printStackTrace();
            }
        }
    }

    public Map<Integer, List<String>> stringPages() {
        return stringPages;
    }
}
