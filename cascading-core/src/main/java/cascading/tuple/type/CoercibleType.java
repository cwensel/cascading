/*
 * Copyright (c) 2007-2022 The Cascading Authors. All Rights Reserved.
 *
 * Project and contact information: https://cascading.wensel.net/
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

package cascading.tuple.type;

import java.io.Serializable;
import java.lang.reflect.Type;

/**
 * Interface CoercibleType allows {@link cascading.tuple.Fields} instances to be extended with custom
 * type information.
 * <p>
 * A CoercibleType is the logical type where the canonical type is the actual type of the value being
 * maintained.
 * <p>
 * It is the role of implementations of this interface to maintain a canonical representation of a given value
 * and to allow for coercions between some type representation to the canonical type and back.
 * <p>
 * For example, if a field in a text delimited file is a date, ie. {@code 28/Dec/2012:16:17:12:931 -0800}
 * it may be beneficial for the internal representation to be a {@link Long} value for performance reasons.
 * <p>
 * Note CoercibleType used in conjunction with the TextDelimited parsers is not a replacement for using
 * a pipe assembly to cleanse data. Pushing data cleansing down to a {@link cascading.tap.Tap} and
 * {@link cascading.scheme.Scheme} may not provide the flexibility and robustness expected.
 * <p>
 * CoercibleTypes are a convenience when the input data is of high quality or was previously written out using
 * a CoercibleType instance.
 * <p>
 * The CoercibleTypes further allow the Cascading planner to perform type checks during joins. If no
 * {@link java.util.Comparator} is in use, and lhs and rhs fields are not the same type, the planner will throw an
 * exception.
 */
public interface CoercibleType<Canonical> extends Type, Serializable
  {
  /** @return the actual Java type this CoercibleType represents */
  Class<Canonical> getCanonicalType();

  /**
   * @param value a value to conform to the current canonical type
   * @return the value coerced into its canonical type
   */
  Canonical canonical( Object value );

  /**
   * Returns a {@link ToCanonical} instance that converts any given value from the given to a
   * canonical representation.
   * <p>
   * This method is equivalent to {@link #canonical(Object)} but optimized when the 'from' type
   * is known ahead of time.
   *
   * @param from the type of the value that will be coerced into its canonical representation
   * @param <T>
   * @return an instance of {@link ToCanonical} optimized for the given from type
   */
  default <T> ToCanonical<T, Canonical> from( Type from )
    {
    if( from == getCanonicalType() )
      return f -> (Canonical) f;

    return this::canonical;
    }

  /**
   * @param value the canonical value
   * @param to    the type to convert the cannonical value to
   * @return the canonical value coerced into the requested type
   */
  <Coerce> Coerce coerce( Object value, Type to );

  /**
   * Returns a {@link CoercionFrom} instance that converts a given canonical value to the requested
   * representation.
   * <p>
   * This method is equivalent to {@link #coerce(Object, Type)} but optimized when the 'to' type
   * is known ahead of time.
   *
   * @param to  the type of the value that the canonical value will be coerced into
   * @param <T>
   * @return an instance of {@link CoercionFrom} optimized for the given from type
   */
  default <T> CoercionFrom<Canonical, T> to( Type to )
    {
    if( to == getCanonicalType() )
      return t -> (T) t;

    return ( t ) -> coerce( t, to );
    }
  }