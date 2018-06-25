package org.apache.ignite.internal.processors.cache.persistence.recovery;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.CheckpointStatus;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointEntryType;
import org.apache.ignite.internal.processors.cache.persistence.file.AsyncFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.lang.IgniteBiTuple;

import static java.nio.file.StandardOpenOption.READ;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.CHECKPOINT_RECORD;
import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.CP_FILE_NAME_PATTERN;

public class CheckpointStateResolver {
    private final FileIOFactory ioFactory = new AsyncFileIOFactory();

    private final IgniteWalIteratorFactory iteratorFactory = new IgniteWalIteratorFactory();

    private final File cpDir;
    private final File walDir;

    public CheckpointStateResolver(File cpDir, File walDir) {
        this.cpDir = cpDir;
        this.walDir = walDir;
    }

    public CheckpointStatus lastCheckpointStatus() throws IgniteCheckedException {
        CheckpointStatus lastCheckpointFromDir = readDirectoryCheckpointStatus();

        return null;
    }

    public List<CheckpointStatus> checkpointStatuses() {
        return null;
    }

    private CheckpointStatus readDirectoryCheckpointStatus() throws IgniteCheckedException {
        long lastStartTs = 0;
        long lastEndTs = 0;

        UUID startId = CheckpointStatus.NULL_UUID;
        UUID endId = CheckpointStatus.NULL_UUID;

        File startFile = null;
        File endFile = null;

        WALPointer startPtr = CheckpointStatus.NULL_PTR;
        WALPointer endPtr = CheckpointStatus.NULL_PTR;

        File dir = cpDir;

        File[] files = dir.listFiles();

        for (File file : files) {
            Matcher matcher = CP_FILE_NAME_PATTERN.matcher(file.getName());

            if (matcher.matches()) {
                long ts = Long.parseLong(matcher.group(1));
                UUID id = UUID.fromString(matcher.group(2));
                CheckpointEntryType type = CheckpointEntryType.valueOf(matcher.group(3));

                if (type == CheckpointEntryType.START && ts > lastStartTs) {
                    lastStartTs = ts;
                    startId = id;
                    startFile = file;
                }
                else if (type == CheckpointEntryType.END && ts > lastEndTs) {
                    lastEndTs = ts;
                    endId = id;
                    endFile = file;
                }
            }
        }

        ByteBuffer buf = ByteBuffer.allocate(20);
        buf.order(ByteOrder.nativeOrder());

        if (startFile != null)
            startPtr = readPointer(startFile, buf);

        if (endFile != null)
            endPtr = readPointer(endFile, buf);

        return new CheckpointStatus(lastStartTs, startId, startPtr, endId, endPtr);
    }

    private WALPointer readPointer(File cpMarkerFile, ByteBuffer buf) throws IgniteCheckedException {
        buf.position(0);

        try (FileIO io = ioFactory.create(cpMarkerFile, READ)) {
            io.read(buf);

            buf.flip();

            return new FileWALPointer(buf.getLong(), buf.getInt(), buf.getInt());
        }
        catch (IOException e) {
            throw new IgniteCheckedException(
                "Failed to read checkpoint pointer from marker file: " + cpMarkerFile.getAbsolutePath(), e);
        }
    }

    private List<CheckpointStatus> readWalCheckpointStatus() throws IgniteCheckedException {
        List<CheckpointStatus> checkpointStatuses = new ArrayList<>();

        try (WALIterator it = iteratorFactory.iterator(walDir)) {
            while (it.hasNext()) {
                IgniteBiTuple<WALPointer, WALRecord> tup = it.next();

                WALRecord rec = tup.get2();

                if (rec.type() == CHECKPOINT_RECORD) {
                    CheckpointRecord cpRec = (CheckpointRecord)rec;
                }
            }
        }
        catch (IgniteException | IgniteCheckedException e) {

        }

        return checkpointStatuses;
    }
}
