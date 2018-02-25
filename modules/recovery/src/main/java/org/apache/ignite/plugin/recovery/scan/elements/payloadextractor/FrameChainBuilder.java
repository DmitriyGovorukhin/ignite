package org.apache.ignite.plugin.recovery.scan.elements.payloadextractor;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class FrameChainBuilder {

    public static final int THRESHOLD = 3072;

    private final Set<Frame> chainHeads = new HashSet<>();

    private final Map<Long, Frame> frames = new HashMap<>();

    public void onNextFrame(long link, byte[] bytes, long nextLink) {
        Frame waiter = frames.remove(link);

        Frame frame = new Frame(link, bytes, nextLink);

        if (frame.payload.length < THRESHOLD)
            frame.head = frame;

        if (waiter == null)
            frames.put(link, frame);
        else {
            waiter.next = frame;

            waiter.len += frame.len;

            frame.head = waiter.head;
        }

        if (nextLink != 0) {
            Frame nextFrame = frames.remove(nextLink);

            if (nextFrame != null) {
                frame.next = nextFrame;

                frame.len += nextFrame.len;

                if (frame.head != null) {
                    Frame tmp = frame.next;

                    while (true) {
                        if (tmp == null)
                            break;

                        tmp.head = frame.head;

                        if (tmp.nextLink == 0) {
                            chainDone(frame.head);

                            break;
                        }

                        tmp = tmp.next;
                    }
                }
            }
            else
                frames.put(nextLink, frame);
        }
        else {
            if (frame.head != null)
                chainDone(frame.head);
        }
    }

    private void chainDone(Frame head) {
        chainHeads.add(head);

        Frame next = head;

        while (true) {
            if (next == null)
                break;

            frames.remove(next.link);

            next = next.next;
        }
    }

    public Set<Frame> completelyChain() {
        return new HashSet<>(chainHeads);
    }
}
