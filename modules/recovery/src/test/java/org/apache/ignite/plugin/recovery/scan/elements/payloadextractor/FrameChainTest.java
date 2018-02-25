package org.apache.ignite.plugin.recovery.scan.elements.payloadextractor;

import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FrameChainTest {

    private final FrameChainBuilder frameChainBuilder = new FrameChainBuilder();

    @Test
    public void test() {
        int links = 5;

        AtomicInteger cnt = new AtomicInteger();

        doTestRecursive(links, new LinkedList<>(), cnt);

        System.out.println("Total tests:" + cnt.get());

        Assertions.assertEquals(5 * 4 * 3 * 2, cnt.get());
    }

    private void doTestRecursive(long totalLinks, LinkedList<Long> links, AtomicInteger cnt) {
        if (totalLinks == links.size()) {
            doTest(links);

            cnt.incrementAndGet();

            return;
        }

        for (long i = 1; i <= totalLinks; i++) {
            if (!links.contains(i)) {
                links.add(i);

                doTestRecursive(totalLinks, links, cnt);

                links.removeLast();
            }
        }
    }

    private void doTest(LinkedList<Long> links) {
        StringBuilder sb = new StringBuilder();

        links.forEach(l -> {
            sb.append(l);

            if (!l.equals(links.getLast()))
                sb.append(" -> ");
        });

        System.out.println(sb);

        byte[] fakePayload = new byte[4000];
        byte[] headPayload = new byte[2000];

        for (Long link : links)
            frameChainBuilder.onNextFrame(link, link == links.size() ? headPayload : fakePayload, link == 1 ? 0 : link - 1);

       // Set<Frame> chain = U.field(frameChainBuilder, "chainHeads");

        // Assertions.assertEquals(1, chain.size(), sb.toString());

        Map<Long, Frame> frames = U.field(frameChainBuilder, "frames");

        Assert.assertEquals(sb.toString(), 1, frames.size());

        Frame head = frames.values().iterator().next();

        //Frame head = chain.iterator().next();

        Frame next = head;

        long idx = next.link;

        while (true) {
            Assertions.assertEquals(idx, next.link);
            Assertions.assertEquals(idx - 1, next.nextLink);

            next = next.next;

            if (next == null)
                break;

            idx = next.link;
        }

        //chain.clear();

        frames.clear();
    }
}
