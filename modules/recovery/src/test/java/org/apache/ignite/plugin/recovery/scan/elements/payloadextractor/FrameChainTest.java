package org.apache.ignite.plugin.recovery.scan.elements.payloadextractor;

import java.util.Map;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FrameChainTest {

    private final FrameChainBuilder frameChainBuilder = new FrameChainBuilder();

    @Test
    public void test() {
        int links = 5;

        int cnt = 0;

        for (int i = 1; i < links; i++) {
            long link1 = i;

            for (int j = 1; j < links; j++) {
                if (j == i)
                    continue;

                long link2 = j;

                for (int k = 1; k < links; k++) {
                    if (k == i || k == j)
                        continue;

                    long link3 = k;

                    for (int m = 1; m < links; m++) {
                        if (m == i || m == j || m == k)
                            continue;

                        long link4 = m;

                        doTest(link1, link2, link3, link4);

                        cnt++;
                    }
                }
            }
        }

        Assertions.assertEquals(4 * 3 * 2, cnt);
    }

    private void doTest(long link1, long link2, long link3, long link4) {
        frameChainBuilder.onNextFrame(link1, null, link1 == 1 ? 0 : link1 - 1);

        frameChainBuilder.onNextFrame(link2, null, link2 == 1 ? 0 : link2 - 1);

        frameChainBuilder.onNextFrame(link3, null, link3 == 1 ? 0 : link3 - 1);

        frameChainBuilder.onNextFrame(link4, null, link4 == 1 ? 0 : link4 - 1);

        Map<Long, Frame> frames = U.field(frameChainBuilder, "frames");

        Assert.assertEquals(link1 + " " + link2 + " " + link3 + " " + link4, 1, frames.size());

        Frame head = frames.values().iterator().next();

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

        frames.clear();
    }
}
