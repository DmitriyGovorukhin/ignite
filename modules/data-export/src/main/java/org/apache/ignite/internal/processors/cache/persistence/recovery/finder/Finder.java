package org.apache.ignite.internal.processors.cache.persistence.recovery.finder;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.util.typedef.F;

import static java.nio.file.Files.walkFileTree;
import static java.nio.file.Paths.get;
import static java.util.Arrays.asList;

public abstract class Finder<T extends Finder.FileDescriptor> {

    public<A extends T> List<A> find(String path, Type... t) {
        if (F.isEmpty(t))
            t = Type.values();

        List<A> res = new ArrayList<>();

        Set<Type> types = new HashSet<>(asList(t));

        try {
            walkFileTree(get(path), new SimpleFileVisitor<Path>() {
                @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    File f = file.toFile();

                    Type type = fileType(f);

                    if (types.contains(type))
                        res.add(createDescriptor(f, type));

                    return FileVisitResult.CONTINUE;
                }
            });
        }
        catch (IOException e) {

        }

        return res;
    }

    protected abstract Type fileType(File file);

    protected abstract<A> A createDescriptor(File file, Type type);

    public enum Type {
        WAL,
        CP,
        NODE_START,
        PAGE_STORE,
        INDEX_STORE,
        UNKNOWN
    }

    public interface FileDescriptor {
        File file();

        Type type();
    }
}
