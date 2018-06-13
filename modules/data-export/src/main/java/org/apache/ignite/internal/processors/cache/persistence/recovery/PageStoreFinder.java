package org.apache.ignite.internal.processors.cache.persistence.recovery;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.PageStoreDescriptor;

public abstract class PageStoreFinder<T extends PageStoreDescriptor> {

    private static final FileFilter FILE_FILTER = file -> isPartition(file.getName());

    private static boolean isPartition(String name) {
        return name.startsWith("part") && name.endsWith(".bin");
    }

    public List<T> findStores(String path) {
        File root = new File(path);

        return root.exists() ? findRecursive(root) : new ArrayList<>();
    }

    private List<T> findRecursive(File file) {
        List<T> res = new ArrayList<>();

        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                res.addAll(findRecursive(f));
            }
        }
        else if (isPartition(file.getName())) {
            T desc = createDescriptor(file);

            if (desc != null)
                res.add(desc);
        }

        return res;
    }

    protected abstract T createDescriptor(File file);

    protected static int partitionIndex(String fileName) {
        String[] split = fileName.split("-");

        return Integer.valueOf(split[1].substring(0, split[1].indexOf(".bin")));
    }
}
