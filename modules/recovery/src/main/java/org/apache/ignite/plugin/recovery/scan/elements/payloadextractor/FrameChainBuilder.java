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

        if (waiter == null)
            frames.put(link, frame);
        else {
            waiter.next = frame;

            waiter.len += frame.len;
        }

        if (nextLink != 0) {
            Frame nextFrame = frames.remove(nextLink);

            if (nextFrame != null) {
                frame.next = nextFrame;

                frame.len += nextFrame.len;
            }
            else
                frames.put(nextLink, frame);
        }
        else {
            if (bytes.length < THRESHOLD) {
                Frame head = frames.remove(link);

                chainHeads.add(head);
            }
        }
    }

    public Set<Frame> completelyChain() {
        return new HashSet<>(chainHeads);
    }
}
