package org.apache.ignite.internal.processors.cache.persistence.recovery;

import junit.framework.TestCase;

public class ArgumentParser extends TestCase {
    public void testParseFindArguments() {
        String find0 = "--find"; // fail
        String find1 = "--find /path";
        String find2 = "--find /path1 /path2";
        String find3 = "--find /path1/part-x.bin /path2";
        String find4 = "--find /path1 /path2/part-x.bin";
    }

    public void testParseCRCArguments() {
        String find0 = "--crc"; //fail
        String find1 = "--crc /path";
        String find2 = "--crc /path1 /path2";
        String find3 = "--crc /path1/part-x.bin /path2";
        String find4 = "--crc /path1 /path2/part-x.bin";
        String find5 = "--crc cacheName:1";
        String find6 = "--crc cacheName:1 cacheName:2";
        String find7 = "--crc cacheName1:1 cacheName2:1";
        String find8 = "--crc cacheName1:1 cacheName2:1";
    }
}
