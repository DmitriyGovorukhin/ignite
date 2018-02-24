package org.apache.ignite.plugin.recovery.scan.elements;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.IncompleteCacheObject;
import org.apache.ignite.internal.processors.cache.IncompleteObject;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.CacheVersionIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPagePayload;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.plugin.recovery.scan.ScanElement;
import sun.nio.ch.DirectBuffer;

public class DataPayloadExtractor implements ScanElement {
    private final int pageSize = 4096;

    private final Set<KeyValue> objects = new HashSet<>();

    private final Map<T2<Long, Integer>, ObjectExtractor> fragments = new HashMap<>();

    @Override public void onNextPage(ByteBuffer buf) {
        if (PageIO.getType(buf) != PageIO.T_DATA)
            return;

        long ptr = ((DirectBuffer)buf).address();

        DataPageIO io = DataPageIO.VERSIONS.forPage(ptr);

        long pageId = PageIO.getPageId(ptr);

        int items = io.getDirectCount(ptr);

        for (int i = 0; i < items; i++) {
            DataPagePayload payload = io.readPayload(ptr, i, pageSize);

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
        ObjectExtractor obj = fragments.get(new T2<>(pageId, item));

        if (obj == null)
            obj = new ObjectExtractor();

        obj.read(buf);

        if (nextLink != 0) {
            long nextPageId = PageIdUtils.pageId(nextLink);
            int nextItemId = PageIdUtils.itemId(nextLink);

            fragments.put(new T2<>(nextPageId, nextItemId), obj);
        }
        else {
            assert obj.isReady() : "object not fully read";

            onObjectRead(obj.toKeyValue());
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

        byte[] value = PageUtils.getBytes(payloadPtr, off, valueLen);

        off += keyLen;

        onObjectRead(new KeyValue(keyType, key, valueType, value));
    }

    private void onObjectRead(KeyValue keyValue) {
        objects.add(keyValue);
    }

    public Set<KeyValue> keyValues() {
        return new HashSet<>(objects);
    }

    /**
     *
     */
    private static class ObjectExtractor {

        private IncompleteCacheObject key;

        private IncompleteObject expireTime;

        private IncompleteCacheObject value;

        private IncompleteObject ver;

        private void read(ByteBuffer buf) {
            if (key == null)
                key = new IncompleteCacheObject(buf);

            key.readData(buf);

            if (!key.isReady())
                return;

            if (expireTime == null) {
                int remaining = buf.remaining();

                if (remaining == 0)
                    return;

                int size = 8;

                expireTime = new IncompleteObject(new byte[size]);
            }

            expireTime.readData(buf);

            if (!expireTime.isReady())
                return;

            if (value == null)
                value = new IncompleteCacheObject(buf);

            value.readData(buf);

            if (!value.isReady())
                return;

            if (ver == null) {
                int remaining = buf.remaining();

                if (remaining == 0)
                    return;

                try {
                    int size = CacheVersionIO.readSize(buf, false);

                    ver = new IncompleteObject(new byte[size]);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }

            ver.readData(buf);

            if (!ver.isReady())
                return;
        }

        public boolean isReady() {
            return key.isReady() && value.isReady();
        }

        private KeyValue toKeyValue() {
            return new KeyValue(key.type(), key.data(), value.type(), value.data());
        }
    }

    /**
     *
     */
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
