package org.apache.ignite.plugin.recovery.scan.elements;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPagePayload;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.plugin.recovery.scan.ScanElement;
import sun.nio.ch.DirectBuffer;

public class DataPayloadExtractor implements ScanElement {
    private final int pageSize = 4096;

    private final Set<KeyValue> objects = new HashSet<>();

    private final Map<T2<Long, Integer>, IncompleteObject> fragments = new HashMap<>();

    @Override public void onNextPage(ByteBuffer buf) {
        if (PageIO.getType(buf) != PageIO.T_DATA)
            return;

        long ptr = ((DirectBuffer)buf).address();

        DataPageIO io = DataPageIO.VERSIONS.forPage(ptr);

        long pageId = PageIO.getPageId(ptr);

        int items = io.getDirectCount(ptr);

        for (int i = 0; i < items; i++) {
            DataPagePayload payload = io.readPayload(ptr, i, pageSize);

            int off = 0;

            long payloadPtr = ptr + payload.offset();

            long nextLink = payload.nextLink();

            if (nextLink != 0) {
                buf.position(payload.offset());
                buf.limit(payload.offset() + payload.payloadSize());

                readFragment(buf, pageId, i, nextLink);
            }
            else
                readFull(payloadPtr);
        }
    }

    private void readFragment(ByteBuffer buf, long pageId, int item, long nextLink) {
        long nextPageId = PageIdUtils.pageId(nextLink);
        int nextItemId = PageIdUtils.itemId(nextLink);

        IncompleteObject obj = fragments.get(new T2<>(pageId, item));

        if (obj == null) {

        }

    }

    private void readFull(long payloadPtr) {
        int off = 0;

        // Read key.
        int keyLen = PageUtils.getInt(payloadPtr, off);

        off += 4;

        byte keyType = PageUtils.getByte(payloadPtr, off);

        off++;

        byte[] key = PageUtils.getBytes(payloadPtr, off, keyLen);

        off += keyLen;

        //Read value.
        int valueLen = PageUtils.getInt(payloadPtr, off);

        off += 4;

        byte valueType = PageUtils.getByte(payloadPtr, off);

        off++;

        byte[] value = PageUtils.getBytes(payloadPtr, off, keyLen);

        off += keyLen;

        onObjectRead(new KeyValue(keyType, key, valueType, value));
    }

    private void onObjectRead(KeyValue keyValue) {
        objects.add(keyValue);
    }

    public Set<KeyValue> keyValues() {
        return new HashSet<>(objects);
    }

    private class IncompleteObject {

        private void read(ByteBuffer buf) {

        }
    }

    public static class KeyValue {
        public final byte keyType;
        public final byte[] keyBytes;

        public final byte valType;
        public final byte[] valBytes;

        private KeyValue(
            byte keyType,
            byte[] keyBytes,
            byte valType,
            byte[] valBytes
        ) {
            this.keyType = keyType;
            this.keyBytes = keyBytes;
            this.valType = valType;
            this.valBytes = valBytes;
        }
    }
}
