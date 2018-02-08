package org.apache.ignite.plugin.recovery;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.lang.IgniteFuture;

public class RecoveryProcessor {
    private final RecoveryConfiguration recoveryConfiguration;

    private final GridKernalContext ctx;

    public RecoveryProcessor(
        RecoveryConfiguration cfg,
        GridKernalContext ctx
    ) {
        this.recoveryConfiguration = cfg;
        this.ctx = ctx;
    }

    public IgniteFuture<?> restoreDataBase() {
        System.err.println("restore database");

        return new IgniteFinishedFutureImpl<>();
    }
}
