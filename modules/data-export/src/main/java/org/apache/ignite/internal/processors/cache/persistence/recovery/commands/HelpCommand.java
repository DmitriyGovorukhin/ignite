package org.apache.ignite.internal.processors.cache.persistence.recovery.commands;

import org.apache.ignite.internal.util.typedef.internal.SB;

public class HelpCommand implements Command {

    @Override public void execute(String... args) {
        SB sb = new SB();

        sb.a("-f --find - find page stores").a("\n");
        sb.a("-c --crc - check pages crc").a("\n");
        sb.a("-e --export - export key/value pair to file").a("\n");
        sb.a("-r --restore - restore binary consistence").a("\n");

        System.out.println(sb);
    }
}
