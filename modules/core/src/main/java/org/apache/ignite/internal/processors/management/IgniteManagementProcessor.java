/**
 * Copyright (C) 2010 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.management;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.management.baseline.BaselineCommand;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Commands annotation reader.
 * Building and managing command registry.
 * Define help usage command (transform command from registry).
 * Proxy task String, BinaryObject for task execution.
 * BinaryObject converters for CLI, REST, JMX (DTO to BinaryObject transformer).
 * CLI adapter.
 * REST adapter.
 * JMX adapter.
 *
 */
public class IgniteManagementProcessor extends GridProcessorAdapter {
    /** Sub-command names separator. */
    private static final String SUBCOMMAND_SEPARATOR = ".";

    /** Map of available commands with key as fully qualified command name. */
    private static final Map<String, CommandDescriptor> registry = new HashMap<>();

    /** @param ctx Kernal context. */
    public IgniteManagementProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        super.start();

        // Add the root command here, all sub-commands will be processed automatically.
        initFromClass(BaselineCommand.class, null, registry::put);
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        registry.clear();
    }

    /**
     * @return Command registry.
     */
    public Map<String, CommandDescriptor> registry() {
        return U.sealMap(registry);
    }

    /**
     * @param clazz Command class.
     * @param parent Parent command descriptor on <tt>null</tt> if there is no parent command.
     * @param registry Ignite command registry.
     */
    private static void initFromClass(
        Class<?> clazz,
        @Nullable CommandDescriptor parent,
        BiConsumer<String, CommandDescriptor> registry
    ) {
        Command ann = U.getAnnotation(clazz, Command.class);

        if (ann == null)
            return;

        CommandDescriptor cmd = new CommandDescriptor(ann, clazz, parent);

        for (Field field : clazz.getDeclaredFields()) {
            if (field.isAnnotationPresent(Parameter.class))
                cmd.params.put(field.getName(), new CommandParameterDescriptor(field));
        }

        if (parent != null)
            parent.subCommands.add(cmd);

        for (Class<?> sub : ann.subcommands())
            initFromClass(sub, cmd, registry);

        registry.accept(cmd.fullyQualifiedName, cmd);
    }

    /** */
    private static class CommandDescriptor {
        /** Command name. */
        private final String name;

        /** Fully qualified command name base on parent command name (e.g. baseline.add). */
        private final String fullyQualifiedName;

        /** Parameter description. */
        private final String[] descr;

        /** Parent command if this descriptor is related to sub-command. */
        @GridToStringInclude
        private final @Nullable CommandDescriptor parent;

        /** Map of command parameters by their names. */
        @GridToStringInclude
        private final Map<String, CommandParameterDescriptor> params = new HashMap<>();

        /** The command class to which this descriptor is related for. */
        private final Class<?> orig;

        /** Sub-commands. */
        @GridToStringInclude
        private final List<CommandDescriptor> subCommands = new ArrayList<>();

        /**
         * @param cmd Command description.
         * @param parent Parent command.
         */
        public CommandDescriptor(Command cmd, Class<?> orig, @Nullable CommandDescriptor parent) {
            assert cmd != null;

            name = cmd.name();
            fullyQualifiedName = parent == null ? name : parent.name + SUBCOMMAND_SEPARATOR + name;
            descr = cmd.commandDescription();
            this.orig = orig;
            this.parent = parent;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CommandDescriptor.class, this);
        }
    }

    /** */
    private static class CommandParameterDescriptor {
        /** An array of allowed command line parameters (e.g. "-p", "--path", etc...). */
        private String[] names;

        /** Parameter description. */
        private String descr;

        /** Field to which this descriptor is related for. */
        private Field field;

        /**
         * @param field Field to which this descriptor is related for.
         */
        public CommandParameterDescriptor(Field field) {
            Parameter param = field.getAnnotation(Parameter.class);

            if (param == null)
                throw new IllegalArgumentException("Parameter annotation not found for field with name: " + field.getName());

            this.names = param.names();
            this.descr = param.description();
            this.field = field;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CommandParameterDescriptor.class, this);
        }
    }
}
