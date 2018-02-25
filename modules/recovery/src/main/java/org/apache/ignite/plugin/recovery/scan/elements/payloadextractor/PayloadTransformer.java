package org.apache.ignite.plugin.recovery.scan.elements.payloadextractor;

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

public class PayloadTransformer {

    private IncompleteCacheObject key;

    private IncompleteObject expireTime;

    private IncompleteCacheObject value;

    private IncompleteObject ver;

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

    private boolean checkChain(Frame head) {
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

    public KeyValue toKeyValue(Frame frame) {
        if (!checkChain(frame))
            return null;

        ByteBuffer buf = GridUnsafe.allocateBuffer(len);

        buf.order(ByteOrder.nativeOrder());

        try {
            Frame frame0 = frame;

            while (true) {
                buf.put(frame0.payload);

                if (frame.nextLink == 0)
                    break;

                frame = frame.next;
            }

            buf.flip();

            assert buf.remaining() == len;

            if (frame.next == null)
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
