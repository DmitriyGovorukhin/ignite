package org.apache.ignite.internal.processors.cache.persistence.recovery.model;

public class Payload {

    private final byte[] payload;

    public Payload(byte[] payload) {
        this.payload = payload;
    }
}
