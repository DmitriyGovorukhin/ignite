package org.apache.ignite.internal.processors.cache.persistence.recovery.commands;

import java.io.IOException;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.processors.cache.persistence.file.AsyncFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.recovery.PageStore;
import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.FilePageStoreDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.FilePageStoreFinder;
import org.apache.ignite.internal.processors.cache.persistence.recovery.scan.PartitionPageStoreScanner;
import org.apache.ignite.internal.processors.cache.persistence.recovery.scan.elements.CorruptedPages;
import org.apache.ignite.internal.processors.cache.persistence.recovery.scan.elements.PageCounter;
import org.apache.ignite.internal.processors.cache.persistence.recovery.scan.elements.ProgressBar;
import org.apache.ignite.internal.processors.cache.persistence.recovery.scan.elements.TimeTracker;
import org.apache.ignite.internal.processors.cache.persistence.recovery.stores.PartitionPageStore;

public class CRCCheckCommand implements Command {
    private final FilePageStoreFinder storeFinder = new FilePageStoreFinder();

    private final FileIOFactory ioFactory = new AsyncFileIOFactory();

    @Override public void execute(String... args) {

        for (FilePageStoreDescriptor desc : resolvePageStoreDescriptors(args)) {
            System.out.println("start CRC checking... ");

            System.out.println(desc);

            checkCRC(desc);

            System.out.println("CRC checking finished...\n");
        }
    }

    private void checkCRC(FilePageStoreDescriptor desc) {
        try (FileIO fileIO = ioFactory.create(desc.file(), StandardOpenOption.READ)) {
            PageStore pageStore = new PartitionPageStore(desc, fileIO);

            PartitionPageStoreScanner pageStoreScanner = new PartitionPageStoreScanner(pageStore);

            CorruptedPages corruptedPageCounter = new CorruptedPages(desc);
            PageCounter pageCounter = new PageCounter();
            TimeTracker timeTracker = new TimeTracker();
            ProgressBar progressBar = new ProgressBar(
                desc.pageSize(),
                desc.version() == 2 ?
                    desc.size() - desc.pageSize() :
                    desc.size() - FilePageStore.HEADER_SIZE
            );

            pageStoreScanner.addHandler(corruptedPageCounter);
            pageStoreScanner.addHandler(pageCounter);
            pageStoreScanner.addHandler(timeTracker);
            pageStoreScanner.addHandler(progressBar);

            pageStoreScanner.scan();

            System.out.println("total execution time: " + (timeTracker.endTime() - timeTracker.startTime()));
            System.out.println("total scanned pages: " + pageCounter.pages());
            System.out.println("corrupted pages: " + corruptedPageCounter.corruptedPages().size());
        }
        catch (IOException e) {
            System.err.println("Error open file=" + desc.file());

            e.printStackTrace(System.err);
        }
    }

    private List<FilePageStoreDescriptor> resolvePageStoreDescriptors(String... paths) {
        List<FilePageStoreDescriptor> res = new ArrayList<>();

        for (String path : paths)
            res.addAll(storeFinder.findStores(path));

        return res;
    }
}
