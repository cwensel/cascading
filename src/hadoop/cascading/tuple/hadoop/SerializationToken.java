/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
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
