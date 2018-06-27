package org.apache.ignite.internal.processors.cache.persistence.recovery.finder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.regex.Matcher;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.filedescriptors.CPDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.filedescriptors.PageStoreDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.recovery.finder.filedescriptors.WALDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.wal.ByteBufferExpander;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileInput;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.SegmentEofException;
import org.apache.ignite.internal.processors.cache.persistence.wal.record.HeaderRecord;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.CP_FILE_NAME_PATTERN;
import static org.apache.ignite.internal.processors.cache.persistence.recovery.finder.Finder.Type.CP;
import static org.apache.ignite.internal.processors.cache.persistence.recovery.finder.Finder.Type.UNKNOWN;
import static org.apache.ignite.internal.processors.cache.persistence.recovery.finder.Finder.Type.WAL;
import static org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.WAL_NAME_PATTERN;
import static org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.WAL_SEGMENT_FILE_COMPACTED_PATTERN;
import static org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer.HEADER_RECORD_SIZE;

public class NodeFilesFinder extends Finder<Finder.FileDescriptor> {

    private final FileIOFactory ioFactory = new DataStorageConfiguration().getFileIOFactory();

    @Override protected Type fileType(File file) {
        String name = file.getName();

        if (isPartition(name))
            return Type.PAGE_STORE;

        if (isCheckpointFile(name))
            return CP;

        if (isWalFile(name))
            return Type.WAL;

        return UNKNOWN;
    }

    @Override protected FileDescriptor createDescriptor(File file, Type type) {
        switch (type) {
            case PAGE_STORE:
                return createPageStoreDescriptor(file);
            case CP:
                return createCheckPointDescriptor(file);
            case WAL:
                return createWALDescriptor(file);
        }

        return null;
    }

    private static boolean isCheckpointFile(String name) {
        return CP_FILE_NAME_PATTERN.matcher(name).matches();
    }

    private static boolean isWalFile(String name) {
        return WAL_NAME_PATTERN.matcher(name).matches() || WAL_SEGMENT_FILE_COMPACTED_PATTERN.matcher(name).matches();
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

    private int partitionIndex(String fileName) {
        String[] split = fileName.split("-");

        return Integer.valueOf(split[1].substring(0, split[1].indexOf(".bin")));
    }

    private CPDescriptor createCheckPointDescriptor(File file) {
        Matcher matcher = CP_FILE_NAME_PATTERN.matcher(file.getName());

        matcher.matches();

        long ts = Long.parseLong(matcher.group(1));
        UUID id = UUID.fromString(matcher.group(2));
        String type = matcher.group(3);

        return new CPDescriptor() {
            @Override public long time() {
                return ts;
            }

            @Override public String cpType() {
                return type;
            }

            @Override public UUID id() {
                return id;
            }

            @Override public File file() {
                return file;
            }

            @Override public Type type() {
                return CP;
            }
        };
    }

    private WALDescriptor createWALDescriptor(File file) {
        long index = 0;

        try (FileIO io = ioFactory.create(file)) {
            index = resolveSegmentIndex(io);
        }
        catch (IOException | SegmentEofException e) {
            System.out.println(e);
        }

        long finalIndex = index;

        return new WALDescriptor() {
            @Override public long segmentIndex() {
                return finalIndex;
            }

            @Override public File file() {
                return file;
            }

            @Override public Type type() {
                return WAL;
            }
        };
    }

    private long resolveSegmentIndex(FileIO io) throws IOException, SegmentEofException {
        try (ByteBufferExpander buf = new ByteBufferExpander(HEADER_RECORD_SIZE, ByteOrder.nativeOrder())) {
            FileInput in = new FileInput(io, buf);

            in.ensure(HEADER_RECORD_SIZE);

            int recordType = in.readUnsignedByte();

            if (recordType == WALRecord.RecordType.STOP_ITERATION_RECORD_TYPE)
                throw new SegmentEofException("Reached logical end of the segment", null);

            WALRecord.RecordType type = WALRecord.RecordType.fromOrdinal(recordType - 1);

            if (type != WALRecord.RecordType.HEADER_RECORD)
                throw new IOException("Can't read serializer version", null);

            long idx = in.readLong();
            int fileOff = in.readInt();

            FileWALPointer ptr = new FileWALPointer(idx, fileOff, 0);

            assert ptr.fileOffset() == 0 : "Header record should be placed at the beginning of file " + ptr;

            long hdrMagicNum = in.readLong();

            boolean compacted;

            if (hdrMagicNum == HeaderRecord.REGULAR_MAGIC)
                compacted = false;
            else if (hdrMagicNum == HeaderRecord.COMPACTED_MAGIC)
                compacted = true;
            else {
                throw new IOException("Magic is corrupted [exp=" + U.hexLong(HeaderRecord.REGULAR_MAGIC) +
                    ", actual=" + U.hexLong(hdrMagicNum) + ']');
            }

            // Read serializer version.
            int ver = in.readInt();

            // Read and skip CRC.
            in.readInt();

            return ptr.index();
        }
    }
}
