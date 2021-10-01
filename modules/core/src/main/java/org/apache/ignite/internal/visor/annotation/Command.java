package org.apache.ignite.internal.visor.annotation;

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
@Target({ElementType.TYPE, ElementType.LOCAL_VARIABLE, ElementType.FIELD, ElementType.PACKAGE, ElementType.METHOD})
public @interface Command {
    /**
     * Program name to show in the synopsis. If omitted, {@code "<main class>"} is used.
     * For {@linkplain #subcommands() declaratively added} subcommands, this attribute is also used
     * by the parser to recognize subcommands in the command line arguments.
     *
     * @return the program name to show in the synopsis
     */
    String name();

    /**
     * A list of classes to instantiate and register as subcommands. When registering subcommands declaratively
     * like this, you don't need to call the {@code CommandLine#addSubcommand(String, Object)} method. For example, this:
     * <pre>
     * &#064;Command(subcommands = {
     *         GitStatus.class,
     *         GitCommit.class,
     *         GitBranch.class })
     * public class Git { ... }
     *
     * CommandLine commandLine = new CommandLine(new Git());
     * </pre> is equivalent to this:
     * <pre>
     * // alternative: programmatically add subcommands.
     * // NOTE: in this case there should be no `subcommands` attribute on the @Command annotation.
     * &#064;Command public class Git { ... }
     *
     * CommandLine commandLine = new CommandLine(new Git())
     *         .addSubcommand("status",   new GitStatus())
     *         .addSubcommand("commit",   new GitCommit())
     *         .addSubcommand("branch",   new GitBranch());
     * </pre>
     * Applications may be interested in the following built-in commands in picocli
     * that can be used as subcommands:
     * <ul>
     *   <li>{@code HelpCommand} - a {@code help} subcommand that prints help on the following or preceding command</li>
     *   <li>{@code AutoComplete.GenerateCompletion} - a {@code generate-completion} subcommand that prints a Bash/ZSH completion
     *   script for its parent command, so that clients can install autocompletion in one line by running
     *   {@code source <(parent-command generate-completion)} in the shell</li>
     * </ul>
     *
     * @return the declaratively registered subcommands of this command, or an empty array if none
     */
    Class<?>[] subcommands() default {};

    /**
     * String that separates options from option parameters. Default is {@code "="}. Spaces are also accepted.
     *
     * @return the string that separates options from option parameters, used both when parsing and when generating usage help
     */
    String separator() default "=";

    /**
     * Set this attribute to {@code true} if this subcommand is a help command, and required options and positional
     * parameters of the parent command should not be validated. If a subcommand marked as {@code helpCommand} is
     * specified on the command line, picocli will not validate the parent arguments (so no "missing required
     * option" errors) and the {@code CommandLine#printHelpIfRequested(List, PrintStream, PrintStream, Help.Ansi)} method will
     * return {@code true}.
     *
     * @return {@code true} if this subcommand is a help command and picocli should not check for missing required
     * options and positional parameters on the parent command
     * @since 3.0
     */
    boolean helpCommand() default false;

    /**
     * Set the heading preceding the header section.
     * <p>May contain embedded {@linkplain java.util.Formatter format specifiers} like {@code %n} line separators.
     * Literal percent {@code '%'} characters must be escaped with another {@code %}.</p>
     *
     * @return the heading preceding the header section
     */
    String headerHeading() default "";

    /**
     * Optional summary description of the command, shown before the synopsis. Each element of the array is rendered on a separate line.
     * <p>May contain embedded {@linkplain java.util.Formatter format specifiers} like {@code %n} line separators. Literal percent
     * {@code '%'} characters must be escaped with another {@code %}.</p>
     *
     * @return summary description of the command
     */
    String[] header() default {};

    /**
     * Set the heading preceding the synopsis text. The default heading is {@code "Usage: "} (without a line break between
     * the heading and the synopsis text).
     * <p>May contain embedded {@linkplain java.util.Formatter format specifiers} like {@code %n} line separators.
     * Literal percent {@code '%'} characters must be escaped with another {@code %}.</p>
     *
     * @return the heading preceding the synopsis text
     */
    String synopsisHeading() default "Usage: ";

    /**
     * Specify {@code true} to generate an abbreviated synopsis like {@code "<main> [OPTIONS] [PARAMETERS...] [COMMAND]"}.
     * By default, a detailed synopsis with individual option names and parameters is generated.
     *
     * @return whether the synopsis should be abbreviated
     */
    boolean abbreviateSynopsis() default false;

    /**
     * Specify one or more custom synopsis lines to display instead of an auto-generated synopsis. Each element of the array
     * is rendered on a separate line.
     * <p>May contain embedded {@linkplain java.util.Formatter format specifiers} like {@code %n} line separators.
     * Literal percent {@code '%'} characters must be escaped with another {@code %}.</p>
     *
     * @return custom synopsis text to replace the auto-generated synopsis
     */
    String[] customSynopsis() default {};

    /**
     * Specify the String to show in the synopsis for the subcommands of this command. The default is
     * {@code "[COMMAND]"}. Ignored if this command has no {@linkplain #subcommands() subcommands}.
     *
     * @since 4.0
     */
    String synopsisSubcommandLabel() default "[COMMAND]";

    /**
     * Set the heading preceding the description section.
     * <p>May contain embedded {@linkplain java.util.Formatter format specifiers} like {@code %n} line separators.
     * Literal percent {@code '%'} characters must be escaped with another {@code %}.</p>
     *
     * @return the heading preceding the description section
     */
    String descriptionHeading() default "";

    /**
     * Optional text to display between the synopsis line(s) and the list of options. Each element of the array is rendered on
     * a separate line.
     * <p>May contain embedded {@linkplain java.util.Formatter format specifiers} like {@code %n} line separators.
     * Literal percent {@code '%'} characters must be escaped with another {@code %}.</p>
     *
     * @return description of this command
     */
    String[] description() default {};

    /**
     * Set the heading preceding the parameters list.
     * <p>May contain embedded {@linkplain java.util.Formatter format specifiers} like {@code %n} line separators.
     * Literal percent {@code '%'} characters must be escaped with another {@code %}.</p>
     *
     * @return the heading preceding the parameters list
     */
    String parameterListHeading() default "";

    /**
     * Set the heading preceding the options list.
     * <p>May contain embedded {@linkplain java.util.Formatter format specifiers} like {@code %n} line separators.
     * Literal percent {@code '%'} characters must be escaped with another {@code %}.</p>
     *
     * @return the heading preceding the options list
     */
    String optionListHeading() default "";

    /**
     * Prefix required options with this character in the options list. The default is no marker: the synopsis
     * indicates which options and parameters are required.
     *
     * @return the character to show in the options list to mark required options
     */
    char requiredOptionMarker() default ' ';

    /**
     * Specify {@code true} to show a {@code [--]} "End of options" entry
     * in the synopsis and option list of the usage help message.
     */
    boolean showEndOfOptionsDelimiterInUsageHelp() default false;

    /**
     * Set the heading preceding the subcommands list. The default heading is {@code "Commands:%n"} (with a line break at the end).
     * <p>May contain embedded {@linkplain java.util.Formatter format specifiers} like {@code %n} line separators.
     * Literal percent {@code '%'} characters must be escaped with another {@code %}.</p>
     *
     * @return the heading preceding the subcommands list
     */
    String commandListHeading() default "Commands:%n";

    /**
     * Set the heading preceding the footer section.
     * <p>May contain embedded {@linkplain java.util.Formatter format specifiers} like {@code %n} line separators.
     * Literal percent {@code '%'} characters must be escaped with another {@code %}.</p>
     *
     * @return the heading preceding the footer section
     */
    String footerHeading() default "";

    /**
     * Optional text to display after the list of options. Each element of the array is rendered on a separate line.
     * <p>May contain embedded {@linkplain java.util.Formatter format specifiers} like {@code %n} line separators.
     * Literal percent {@code '%'} characters must be escaped with another {@code %}.</p>
     *
     * @return text to display after the list of options
     */
    String[] footer() default {};

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
}
