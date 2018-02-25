package org.apache.ignite.plugin.recovery.scan.elements.payloadextractor;

public class KeyValue {
    public final byte keyType;
    public final byte[] key;

    public final byte valueType;
    public final byte[] value;

    public KeyValue(byte keyType, byte[] key, byte valueType, byte[] value) {
        this.keyType = keyType;
        this.key = key;
        this.valueType = valueType;
        this.value = value;
    }
}
