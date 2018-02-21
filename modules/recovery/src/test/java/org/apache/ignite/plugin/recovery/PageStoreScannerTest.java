package org.apache.ignite.plugin.recovery;

import java.io.IOException;
import junit.framework.TestCase;
import org.apache.ignite.plugin.recovery.scan.PageStoreScanner;
import org.apache.ignite.plugin.recovery.scan.elements.PagesByType;

public class PageStoreScannerTest extends TestCase {
    private static final String FILE = "/path";

    public void test() throws IOException {
        PageStoreScanner pageStoreScanner = PageStoreScanner.create(FILE);

        PagesByType pagesByType = new PagesByType();

        pageStoreScanner.addElement(pagesByType);

        pageStoreScanner.scan();
    }
}
