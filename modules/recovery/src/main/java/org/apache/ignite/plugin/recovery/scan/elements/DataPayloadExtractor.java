package org.apache.ignite.plugin.recovery.scan.elements;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
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
import org.apache.ignite.internal.util.GridUnsafe;
import sun.nio.ch.DirectBuffer;

public class DataPayloadExtractor extends ScanAdapter {
    private final int pageSize = 4096;

    private final Set<KeyValue> objects = new HashSet<>();

    private final Map<Long, ObjectExtractor> fragments = new HashMap<>();

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

            long link = PageIdUtils.link(pageId, i);

            ObjectExtractor objectExtractor = fragments.get(link);

            if (objectExtractor == null)
                fragments.put(link, objectExtractor = new ObjectExtractor());

            byte[] bytes = PageUtils.getBytes(payloadPtr, 0, payload.payloadSize());

            Frame frame = new Frame(link, bytes, nextLink);

            if (nextLink != 0) {
                ObjectExtractor objectExtractorNext = fragments.remove(nextLink);

                if (objectExtractorNext != null){
                    objectExtractorNext.onNextFrame(frame);

                    objectExtractor.merge(objectExtractorNext);
                }
                else{
                    objectExtractor.onNextFrame(frame);

                    fragments.put(nextLink, objectExtractor);
                }
            }
            else
                objectExtractor.onNextFrame(frame);

            if (items > 1)
                if (objectExtractor.checkChain()) {
                    onObjectRead(objectExtractor.toKeyValue());

                    fragments.remove(link);
                    fragments.remove(nextLink);
                }
        }
    }

    @Override public void onComplete() {
        for (ObjectExtractor obj : fragments.values())
            onObjectRead(obj.toKeyValue());

        fragments.clear();
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

        private Frame head;

        private int len;

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

        public void onNextFrame(Frame frame) {
            if (head == null)
                head = frame;
            else {
                if (head.nextLink == frame.link)
                    head.next = frame;
                else if (frame.nextLink == head.link) {
                    frame.next = head;

                    head = frame;
                }
            }
        }

        private boolean checkChain() {
            Frame frame = head;

            int len = 0;

            while (true) {
                len += frame.payload.length;

                if (frame.nextLink == 0) {
                    this.len = len;

                    return true;
                }

                frame = frame.next;

                if (frame == null)
                    return false;
            }
        }

        public void merge(ObjectExtractor objectExtractor) {
            if (head == null)
                head = objectExtractor.head;
            else {
                if (head.nextLink == objectExtractor.head.link)
                    head.next = objectExtractor.head;
                else if (objectExtractor.head.nextLink == head.link) {
                    objectExtractor.head = head;

                    head = objectExtractor.head;
                }
            }
        }

        private KeyValue toKeyValue() {
            if (!checkChain())
                return null;

            ByteBuffer buf = GridUnsafe.allocateBuffer(len);

            buf.order(ByteOrder.nativeOrder());

            try {
                Frame frame = head;

                while (true) {
                    buf.put(frame.payload);

                    if (frame.nextLink == 0)
                        break;

                    frame = frame.next;
                }

                buf.flip();

                assert buf.remaining() == len;

                if (head.next == null)
                    return readFull(((DirectBuffer)buf).address());
                else {
                    read(buf);

                    return new KeyValue(key.type(), key.data(), value.type(), value.data());
                }
            }
            finally {
                GridUnsafe.freeBuffer(buf);
            }
        }

        private KeyValue readFull(long payloadPtr) {
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

            return new KeyValue(keyType, key, valueType, value);
        }
    }

    /**
     *
     */
    private static class Frame {

        private final long link;

        private final byte[] payload;

        private final long nextLink;

        private Frame next;

        private Frame(long link, byte[] payload, long nextLink) {
            this.link = link;
            this.payload = payload;
            this.nextLink = nextLink;
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

        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            KeyValue value = (KeyValue)o;

            if (keyType != value.keyType)
                return false;
            if (valType != value.valType)
                return false;
            if (!Arrays.equals(keyBytes, value.keyBytes))
                return false;
            return Arrays.equals(valBytes, value.valBytes);
        }

        @Override public int hashCode() {
            int result = (int)keyType;
            result = 31 * result + Arrays.hashCode(keyBytes);
            result = 31 * result + (int)valType;
            result = 31 * result + Arrays.hashCode(valBytes);
            return result;
        }
    }
}
