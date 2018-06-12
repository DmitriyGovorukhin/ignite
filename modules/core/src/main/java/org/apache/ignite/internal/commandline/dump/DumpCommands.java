package org.apache.ignite.internal.commandline.dump;

import org.jetbrains.annotations.Nullable;

public enum DumpCommands {
    /**
     * Prints out help for the dump command.
     */
    HELP("help"),

    INFO("info"),

    CRC_CHECK("crc"),

    EXTRACT("extract");

    /** Enumerated values. */
    private static final DumpCommands[] VALS = values();

    /** Name. */
    private final String name;

    /**
     * @param name Name.
     */
    DumpCommands(String name) {
        this.name = name;
    }

    /**
     * @param text Command text.
     * @return Command for the text.
     */
    public static DumpCommands of(String text) {
        for (DumpCommands cmd : DumpCommands.values()) {
            if (cmd.text().equalsIgnoreCase(text))
                return cmd;
        }

        return null;
    }

    /**
     * @return Name.
     */
    public String text() {
        return name;
    }

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static DumpCommands fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
