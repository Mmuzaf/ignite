package org.apache.ignite.internal.management.baseline;

import org.apache.ignite.internal.management.Command;
import org.apache.ignite.internal.management.IgniteCommand;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
@Command(name = "baseline",
    commandDescription = "Baseline Command",
    subcommands = {})
public class BaselineCommand implements IgniteCommand<String, String> {
    /** {@inheritDoc} */
    @Override public String call() throws Exception {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean collect(@Nullable String s) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public String reduce() {
        return null;
    }
}
