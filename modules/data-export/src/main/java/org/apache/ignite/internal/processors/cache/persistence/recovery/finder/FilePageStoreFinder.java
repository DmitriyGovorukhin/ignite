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
import org.apache.ignite.internal.processors.cache.persistence.recovery.PageStoreFinder;

public class FilePageStoreFinder extends PageStoreFinder<FilePageStoreDescriptor> {

    private final FileIOFactory ioFactory = new DataStorageConfiguration().getFileIOFactory();

    @Override protected FilePageStoreDescriptor createDescriptor(File file) {
        try (FileIO fileIO = ioFactory.create(file, StandardOpenOption.READ)){
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

            return new FilePageStoreDescriptor() {
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

                @Override public byte type() {
                    return type;
                }

                @Override public String cacheOrGroupName() {
                    return cacheOrGroupName;
                }
            };

        }
        catch (IOException e) {
            return null;
        }
    }
}
