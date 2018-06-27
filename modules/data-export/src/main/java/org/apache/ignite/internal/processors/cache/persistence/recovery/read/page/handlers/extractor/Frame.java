package org.apache.ignite.internal.processors.cache.persistence.recovery.read.page.handlers.extractor;

public class Frame {
    final long link;

    final byte[] payload;

    final long nextLink;

    Frame next;

    Frame head;

    int len;

    Frame(long link, byte[] payload, long nextLink) {
        this.link = link;
        this.payload = payload;
        this.nextLink = nextLink;
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        Frame frame = (Frame)o;

        if (link != frame.link)
            return false;

        return nextLink == frame.nextLink;
    }

    @Override public int hashCode() {
        int result = (int)(link ^ (link >>> 32));

        result = 31 * result + (int)(nextLink ^ (nextLink >>> 32));

        return result;
    }
}
