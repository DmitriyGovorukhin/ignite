package org.apache.ignite.internal.commandline.dump;

public class DumpArguments {
    /** Command. */
    private DumpCommands cmd;

    public void command(DumpCommands cmd) {
        this.cmd = cmd;
    }

    public DumpCommands command() {
        return cmd;
    }
}
