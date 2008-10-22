/*
 * Copyright (c) 2007-2008 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
 */

package cascading.tuple.hadoop;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation SerializationToken enables {@link cascading.tuple.TupleInputStream} and {@link cascading.tuple.TupleOutputStream}
 * to substitute Integer values for a class name when writing out nested objects inside a {@link cascading.tuple.Tuple}.
 * <p/>
 * To use, add this annotation to any custom Hadoop {@link org.apache.hadoop.io.serializer.Serialization} implementation.
 * <p/>
 * For example:<br/>
 * <pre>
 * &#64;SerializationToken(tokens={222, 223}, classNames = {"example.PersonObject", "example.SiteObject"})
 * public class MySerialization implements org.apache.hadoop.io.serializer.Serialization
 * {
 * public MySerialization()
 * {
 * ...
 * }
 * ...
 * }
 * </pre>
 * <p/>
 * The SerializationToken annotation allows for multiple token to className mappings, since a Serialization implementation may
 * {@link org.apache.hadoop.io.serializer.Serialization#accept(Class)} more than one class.
 * <p/>
 * Note that the token integer value must be 128 or greater to save room for internal types.
 *
 * @see cascading.tuple.TupleInputStream
 * @see cascading.tuple.TupleOutputStream
 * @see TupleSerialization
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface SerializationToken
  {
  int[] tokens();

  String[] classNames();
  }
