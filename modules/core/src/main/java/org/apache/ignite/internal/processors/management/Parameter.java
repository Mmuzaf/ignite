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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 *
 */
@Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD, ElementType.METHOD })
public @interface Parameter {
  /**
   * An array of allowed command line parameters (e.g. "-d", "--outputdir", etc...).
   * If this attribute is omitted, the field it's annotating will receive all the
   * unparsed options. There can only be at most one such annotation.
   */
  String[] names() default {};

  /**
   * A description of this parameter.
   */
  String description() default "";

  /**
   * Whether this parameter is required.
   */
  boolean required() default false;

  /** <p>
   * Optionally specify a {@code type} to control exactly what Class the option parameter should be converted
   * to. This may be useful when the field type is an interface or an abstract class. For example, a field can
   * be declared to have type {@code java.lang.Number}, and annotating {@code @Option(type=Short.class)}
   * ensures that the option parameter value is converted to a {@code Short} before setting the field value.
   * </p><p>
   * For array fields whose <em>component</em> type is an interface or abstract class, specify the concrete <em>component</em> type.
   * For example, a field with type {@code Number[]} may be annotated with {@code @Option(type=Short.class)}
   * to ensure that option parameter values are converted to {@code Short} before adding an element to the array.
   * </p><p>
   * Picocli will use the {@code ITypeConverter} that is
   * {@code #registerConverter(Class, ITypeConverter) registered} for the specified type to convert
   * the raw String values before modifying the field value.
   * </p><p>
   * Prior to 2.0, the {@code type} attribute was necessary for {@code Collection} and {@code Map} fields,
   * but starting from 2.0 picocli will infer the component type from the generic type's type arguments.
   * For example, for a field of type {@code Map<TimeUnit, Long>} picocli will know the option parameter
   * should be split up in key=value pairs, where the key should be converted to a {@code java.util.concurrent.TimeUnit}
   * enum value, and the value should be converted to a {@code Long}. No {@code @Option(type=...)} type attribute
   * is required for this. For generic types with wildcards, picocli will take the specified upper or lower bound
   * as the Class to convert to, unless the {@code @Option} annotation specifies an explicit {@code type} attribute.
   * </p><p>
   * If the field type is a raw collection or a raw map, and you want it to contain other values than Strings,
   * or if the generic type's type arguments are interfaces or abstract classes, you may
   * specify a {@code type} attribute to control the Class that the option parameter should be converted to.
   * @return the type(s) to convert the raw String values
   */
//  Class<?>[] type() default {};

//    /**
//     * Optionally specify one or more {@link ITypeConverter} classes to use to convert the command line argument into
//     * a strongly typed value (or key-value pair for map fields). This is useful when a particular field should
//     * use a custom conversion that is different from the normal conversion for the field's type.
//     * <p>For example, for a specific field you may want to use a converter that maps the constant names defined
//     * in {@link java.sql.Types java.sql.Types} to the {@code int} value of these constants, but any other {@code int} fields should
//     * not be affected by this and should continue to use the standard int converter that parses numeric values.</p>
//     * @return the type converter(s) to use to convert String values to strongly typed values for this field
//     * @see CommandLine#registerConverter(Class, ITypeConverter)
//     */
//    Class<? extends ITypeConverter<?>>[] converter() default {};


  /**
   * The string converter to use for this field. If the field is of type List
   * and not listConverter attribute was specified, JCommander will split
   * the input in individual values and convert each of them separately.
   */
//  Class<? extends IStringConverter<?>> converter() default NoConverter.class;

  /**
   * The list string converter to use for this field. If it's specified, the
   * field has to be of type List and the converter needs to return
   * a List that's compatible with that type.
   */
//  Class<? extends IStringConverter<?>> listConverter() default NoConverter.class;

  /**
   * Validate the parameter found on the command line.
   */
//  Class<? extends IParameterValidator>[] validateWith() default NoValidator.class;

  /**
   * Validate the value for this parameter.
   */
//  Class<? extends IValueValidator>[] validateValueWith() default NoValueValidator.class;

  /**
   * What splitter to use (applicable only on fields of type List). By default,
   * a comma separated splitter will be used.
   */
//  Class<? extends IParameterSplitter> splitter() default CommaParameterSplitter.class;
}
