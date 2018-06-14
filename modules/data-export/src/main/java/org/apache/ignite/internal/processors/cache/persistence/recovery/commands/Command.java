package org.apache.ignite.internal.processors.cache.persistence.recovery.commands;

public interface Command {

    void execute(String... args);
}
