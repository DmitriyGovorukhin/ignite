package org.apache.ignite.internal.processors.cache.persistence.recovery.scan.elements.extractor;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.IncompleteCacheObject;
import org.apache.ignite.internal.processors.cache.IncompleteObject;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.CacheVersionIO;
import org.apache.ignite.internal.util.GridUnsafe;
import sun.nio.ch.DirectBuffer;

public class FrameTransformer {

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
    }

    public boolean isReady() {
        return key.isReady() && value.isReady();
    }


    public KeyValue toKeyValue(Frame head) {
        ByteBuffer buf = GridUnsafe.allocateBuffer(head.len);

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

            assert buf.remaining() == head.len;

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
