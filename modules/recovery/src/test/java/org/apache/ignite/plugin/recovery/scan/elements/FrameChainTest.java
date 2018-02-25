package org.apache.ignite.plugin.recovery.scan.elements;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class FrameChainTest {

    @Test
    public void test() {
        int links = 8;

        AtomicInteger cnt = new AtomicInteger();

        doTestRecursive(links, new LinkedList<>(), cnt);

        System.out.println("Total tests:" + cnt.get());

        assertEquals( 8 * 7 * 6 * 5 * 4 * 3 * 2, cnt.get());
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
        Set<Frame> chains = new HashSet<>();

        FrameChainBuilder frameChainBuilder = new FrameChainBuilder(chains::add);

        StringBuilder sb = new StringBuilder();

        links.forEach(l -> {
            sb.append(l);

            if (!l.equals(links.getLast()))
                sb.append(" -> ");
        });

        System.out.println(sb);

        byte[] fakePayload = new byte[4030];
        byte[] headPayload = new byte[2000];

        for (Long link : links)
            frameChainBuilder.onNextFrame(link, link == links.size() ? headPayload : fakePayload, link == 1 ? 0 : link - 1);

        assertEquals(1, chains.size(), sb.toString());

        Map<Long, Frame> frames = U.field(frameChainBuilder, "frames");

        assertEquals(sb.toString(), 0, frames.size());

        //Frame head = frames.values().iterator().next();

        Frame head = chains.iterator().next();

        Frame frame = head;

        long link = frame.link;

        while (true) {
            Assertions.assertEquals(link, frame.link);
            Assertions.assertEquals(link - 1, frame.nextLink);

            if (head == frame)
                Assertions.assertEquals((link - 1) * fakePayload.length + headPayload.length, frame.len);
            else
                Assertions.assertEquals(link * fakePayload.length, frame.len);

            frame = frame.next;

            if (frame == null)
                break;

            link = frame.link;
        }
    }
}
