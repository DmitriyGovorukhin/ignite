package org.apache.ignite.internal.processors.cache.persistence.recovery;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.recovery.commands.CRCCheckCommand;
import org.apache.ignite.internal.processors.cache.persistence.recovery.commands.Command;
import org.apache.ignite.internal.processors.cache.persistence.recovery.commands.DataExportCommand;
import org.apache.ignite.internal.processors.cache.persistence.recovery.commands.FindStoreCommand;
import org.apache.ignite.internal.processors.cache.persistence.recovery.commands.HelpCommand;
import org.apache.ignite.internal.util.typedef.F;

import static java.util.Arrays.copyOfRange;

public class Main {

    public static void main(String[] args) throws IgniteCheckedException {
        if (F.isEmpty(args)){
            System.out.println("Command is not specific. Try to use --help for more information.");

            return;
        }

        System.out.println(Arrays.toString(args));

        String command = args[0];

        Map<String, Command> COMMANDS = new HashMap<>();

        COMMANDS.put("--find", new FindStoreCommand());
        COMMANDS.put("--export", new DataExportCommand());
        COMMANDS.put("--crc", new CRCCheckCommand());
        COMMANDS.put("--help", new HelpCommand());

        Command cmd = COMMANDS.get(command);

        if (cmd != null)
            cmd.execute(copyOfRange(args, 1, args.length));
        else
            throw new IgniteCheckedException("Command '" + command + "'");
    }
}
