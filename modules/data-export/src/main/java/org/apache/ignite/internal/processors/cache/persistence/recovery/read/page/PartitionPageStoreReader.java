package org.apache.ignite.internal.processors.cache.persistence.recovery.read.page;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.recovery.store.page.PageIterator;
import org.apache.ignite.internal.processors.cache.persistence.recovery.read.StoreReader;
import org.apache.ignite.internal.processors.cache.persistence.recovery.read.ReadHandler;
import org.apache.ignite.internal.processors.cache.persistence.recovery.store.page.PartitionPageStore;
import org.apache.ignite.internal.util.GridUnsafe;

public class PartitionPageStoreReader implements StoreReader<ByteBuffer> {
    private final List<ReadHandler<ByteBuffer>> readHandlers = new ArrayList<>();

    private final PartitionPageStore store;

    public PartitionPageStoreReader(PartitionPageStore store) {
        this.store = store;
    }

    @Override public void addHandler(ReadHandler<ByteBuffer> e) {
        readHandlers.add(e);
    }

    @Override public void read() {
        int pageSize = store.descriptor().pageSize();

        PageIterator it = store.iterator();

        ByteBuffer buf = ByteBuffer.allocate(pageSize);

        buf.order(ByteOrder.nativeOrder());

        ByteBuffer tmp = GridUnsafe.allocateBuffer(pageSize);

        tmp.order(ByteOrder.nativeOrder());

        try {
            while (it.hasNext()) {
                try {
                    it.next(buf);
                }
                catch (IgniteCheckedException e) {
                    e.printStackTrace();
                }

                buf.flip();

                for (ReadHandler h : readHandlers) {
                    tmp.put(buf);

                    tmp.rewind();

                    h.onNextRead(tmp);

                    tmp.clear();
                }

                buf.clear();
            }

            for (ReadHandler e : readHandlers)
                e.onComplete();
        }
        finally {
            GridUnsafe.freeBuffer(tmp);
        }
    }
}
