package org.apache.ignite.internal.processors.cache.persistence.recovery.finder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.StandardOpenOption;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.descriptors.PageStoreDescriptor;

public class NodeFilesFinder extends Finder<Finder.Descriptor> {

    private final FileIOFactory ioFactory = new DataStorageConfiguration().getFileIOFactory();


    @Override protected Type fileType(File file) {
        String name = file.getName();

        if (isPartition(name))
            return Type.PAGE_STORE;

        return null;
    }

    @Override protected Descriptor createDescriptor(File file, Type type) {
        switch (type){
            case PAGE_STORE:
                return createPageStoreDescriptor(file);
            case CP:
        }
        return null;
    }


    private static boolean isCheckpointFile(String name){
        return name.startsWith("part") && name.endsWith(".bin");
    }

    private static boolean isPartition(String name) {
        return name.startsWith("part") && name.endsWith(".bin");
    }

    private PageStoreDescriptor createPageStoreDescriptor(File file) {
        try (FileIO fileIO = ioFactory.create(file, StandardOpenOption.READ)) {
            int header = FilePageStore.HEADER_SIZE;

            ByteBuffer buf = ByteBuffer.allocate(header).order(ByteOrder.LITTLE_ENDIAN);

            fileIO.read(buf);

            buf.rewind();

            long signature = buf.getLong();

            int version = buf.getInt();

            byte type = buf.get();

            int pageSize = buf.getInt();

            String cacheOrGroupName = file.getParentFile().getName();

            long size = file.length();

            int partitionIndex = partitionIndex(file.getName());

            return new PageStoreDescriptor() {
                @Override public File file() {
                    return file;
                }

                @Override public int partitionId() {
                    return partitionIndex;
                }

                @Override public long size() {
                    return size;
                }

                @Override public int pageSize() {
                    return pageSize;
                }

                @Override public int version() {
                    return version;
                }

                @Override public Type type() {
                    return Type.PAGE_STORE;
                }

                @Override public String cacheOrGroupName() {
                    return cacheOrGroupName;
                }

                @Override public String toString() {
                    return file.getAbsolutePath() + "\nname " + cacheOrGroupName
                        + "\nidx " + partitionIndex + "\ntype " + type
                        + "\nsize " + size + "\npageSize " + pageSize
                        + "\nversion " + version;
                }
            };

        }
        catch (IOException e) {
            return null;
        }
    }


    protected static int partitionIndex(String fileName) {
        String[] split = fileName.split("-");

        return Integer.valueOf(split[1].substring(0, split[1].indexOf(".bin")));
    }
}
