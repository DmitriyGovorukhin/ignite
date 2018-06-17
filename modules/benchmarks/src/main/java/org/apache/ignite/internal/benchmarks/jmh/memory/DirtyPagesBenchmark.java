package org.apache.ignite.internal.benchmarks.jmh.memory;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.internal.benchmarks.jmh.JmhAbstractBenchmark;
import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryImpl;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

@State(Scope.Benchmark)
public class DirtyPagesBenchmark extends JmhAbstractBenchmark {

    private static final long PAGE_SIZE = 4096;

    private static final long TOTAL_MEMORY = 1024 * 1024 * 1024L;

    private static final long MAX_PAGE_ID = TOTAL_MEMORY / PAGE_SIZE;

    private PageMemoryImpl.DirtyPages dirtyPages;

    /**
     * @throws Exception If failed.
     */
    @Setup
    public void setup() throws Exception {
      //  dirtyPages = new PageMemoryImpl.DirtyPagesBitSet(TOTAL_MEMORY, 4096);
        dirtyPages = new PageMemoryImpl.DirtyPagesConcurrentSet();
    }


    @Benchmark
    public Boolean add() throws Exception {
        Long pageId = ThreadLocalRandom.current().nextLong(MAX_PAGE_ID);

        return dirtyPages.add(new FullPageId(pageId, 0));
    }

    @Benchmark
    public Boolean remove() throws Exception {
        Long pageId = ThreadLocalRandom.current().nextLong(MAX_PAGE_ID);

        return dirtyPages.remove(new FullPageId(pageId, 0));
    }

    @Benchmark
    public Boolean contains() throws Exception {
        Long pageId = ThreadLocalRandom.current().nextLong(MAX_PAGE_ID);

        return dirtyPages.remove(new FullPageId(pageId, 0));
    }

    @Benchmark
    public Boolean randomOperation() throws Exception {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        Long pageId = rnd.nextLong(MAX_PAGE_ID);

        int op = rnd.nextInt(3);

        FullPageId page = new FullPageId(pageId, 0);

        if (op == 0)
            return dirtyPages.add(page);
        else if (op == 1)
            return dirtyPages.remove(page);
        else if (op == 2)
            return dirtyPages.contains(page);

        return Boolean.TRUE;
    }

    /**
     * Run benchmarks.
     *
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        run(8);
    }

    /**
     * Run benchmark.
     *
     * @param threads Amount of threads.
     * @throws Exception If failed.
     */
    private static void run(int threads) throws Exception {
        JmhIdeBenchmarkRunner.create()
            .forks(1)
            .threads(threads)
            .warmupIterations(10)
            .measurementIterations(10)
            .benchmarks(DirtyPagesBenchmark.class.getSimpleName())
            .jvmArguments("-Xms4g", "-Xmx4g")
            .run();
    }
}
