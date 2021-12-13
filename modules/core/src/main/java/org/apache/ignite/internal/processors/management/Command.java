/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.management;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <p>Annotate your class with {@code @Command} when you want more control over the format of the generated help
 * message. From 3.6, methods can also be annotated with {@code @Command}, where the method parameters define the
 * command options and positional parameters.
 * </p><pre>
 * &#064;Command(name              = "Encrypt", mixinStandardHelpOptions = true,
 *        description         = "Encrypt FILE(s), or standard input, to standard output or to the output file.",
 *        version             = "Encrypt version 1.0",
 *        footer              = "Copyright (c) 2017",
 *        exitCodeListHeading = "Exit Codes:%n",
 *        exitCodeList        = { " 0:Successful program execution.",
 *                                "64:Invalid input: an unknown option or invalid parameter was specified.",
 *                                "70:Execution exception: an exception occurred while executing the business logic."}
 *        )
 * public class Encrypt {
 *     &#064;Parameters(paramLabel = "FILE", description = "Any number of input files")
 *     private List&lt;File&gt; files = new ArrayList&lt;File&gt;();
 *
 *     &#064;Option(names = { "-o", "--out" }, description = "Output file (default: print to console)")
 *     private File outputFile;
 *
 *     &#064;Option(names = { "-v", "--verbose"}, description = "Verbose mode. Helpful for troubleshooting. Multiple -v options
 *     increase the verbosity.")
 *     private boolean[] verbose;
 * }</pre>
 * <p>
 * The structure of a help message looks like this:
 * </p><ul>
 * <li>[header]</li>
 * <li>[synopsis]: {@code Usage: <commandName> [OPTIONS] [FILE...]}</li>
 * <li>[description]</li>
 * <li>[parameter list]: {@code      [FILE...]   Any number of input files}</li>
 * <li>[option list]: {@code   -h, --help   prints this help message and exits}</li>
 * <li>[exit code list]</li>
 * <li>[footer]</li>
 * </ul>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Command {
    /**
     * An array of allowed command names.
     */
    String name();

    /**
     * Alternative command names by which this subcommand is recognized on the command line.
     */
    String[] aliases() default {};

    /**
     * A list of classes to instantiate and register as subcommands. When registering subcommands declaratively
     * like this, you don't need to call the {@code CommandLine#addSubcommand(String, Object)} method. For example, this:
     */
    Class<?>[] subcommands() default {};

    /**
     * @return Command description. Each string will be printed on a new line.
     */
    String[] commandDescription() default "";

    /**
     * The characters that separate options, spaces are also supported.
     */
    String separator() default "=";

    /**
     * Set the values to be displayed in the exit codes section as a list of {@code "key:value"} pairs:
     * keys are exit codes, values are descriptions. Descriptions may contain {@code "%n"} line separators.
     * <p>For example:</p>
     * <pre>
     * &#064;Command(exitCodeListHeading = "Exit Codes:%n",
     *          exitCodeList = { " 0:Successful program execution.",
     *                           "64:Invalid input: an unknown option or invalid parameter was specified.",
     *                           "70:Execution exception: an exception occurred while executing the business logic."})
     * </pre>
     */
    String[] exitCodeList() default {};

    /**
     * The name of the resource bundle to use for this class.
     */
    String resourceBundle() default "";
}
