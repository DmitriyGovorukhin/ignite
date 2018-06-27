package org.apache.ignite.internal.processors.cache.persistence.recovery.commands;

import java.io.IOException;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.persistence.file.AsyncFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.recovery.read.StoreReader;
import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.Finder;
import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.NodeFilesFinder;
import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.filedescriptors.PageStoreDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.recovery.read.page.PartitionPageStoreReader;
import org.apache.ignite.internal.processors.cache.persistence.recovery.read.page.handlers.CorruptedPages;
import org.apache.ignite.internal.processors.cache.persistence.recovery.read.page.handlers.PageCounter;
import org.apache.ignite.internal.processors.cache.persistence.recovery.read.page.handlers.ProgressBar;
import org.apache.ignite.internal.processors.cache.persistence.recovery.read.page.handlers.TimeTracker;
import org.apache.ignite.internal.processors.cache.persistence.recovery.store.page.PartitionPageStore;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.internal.processors.cache.persistence.recovery.finder.Finder.Type.PAGE_STORE;

public class CRCCheckCommand implements Command {
    private final NodeFilesFinder storeFinder = new NodeFilesFinder();

    private final FileIOFactory ioFactory = new AsyncFileIOFactory();

    @Override public void execute(String... args) {
        String path = args[0];

        List<PageStoreDescriptor> descs = storeFinder.find(path, PAGE_STORE);

        if (F.isEmpty(descs))
            throw new IgniteException("Page store not found, path=" + path);

        for (PageStoreDescriptor desc : descs) {
            System.out.println("CRC checking");

            System.out.println(desc);

            checkCRC(desc);
        }
    }

    private void checkCRC(PageStoreDescriptor desc) {
        try (FileIO fileIO = ioFactory.create(desc.file(), StandardOpenOption.READ)) {
            PartitionPageStore store = new PartitionPageStore(desc, fileIO);

            PartitionPageStoreReader pageStoreScanner = StoreReader.create(store);

            CorruptedPages corruptedPageCounter = new CorruptedPages(desc);
            PageCounter pageCounter = new PageCounter();
            TimeTracker timeTracker = new TimeTracker();
            ProgressBar progressBar = new ProgressBar(desc);

            pageStoreScanner.addHandler(corruptedPageCounter);
            pageStoreScanner.addHandler(pageCounter);
            pageStoreScanner.addHandler(timeTracker);
            pageStoreScanner.addHandler(progressBar);

            pageStoreScanner.read();

            System.out.println("total execution time: " + (timeTracker.endTime() - timeTracker.startTime()));
            System.out.println("total scanned pages: " + pageCounter.pages());
            System.out.println("corrupted pages: " + corruptedPageCounter.corruptedPages().size());
        }
        catch (IOException e) {
            System.err.println("Error open file=" + desc.file());

            e.printStackTrace(System.err);
        }
    }
}
