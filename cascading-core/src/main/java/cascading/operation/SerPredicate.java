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

package cascading.operation;

import java.io.Serializable;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * Interface SerPredicate is a {@link Serializable} {@link Predicate} functional interface.
 *
 * @param <T>
 */
@FunctionalInterface
public interface SerPredicate<T> extends Predicate<T>, Serializable
  {
  static <T> Predicate<T> isEqual( Object targetRef )
    {
    return ( null == targetRef )
      ? Objects::isNull
      : targetRef::equals;
    }

  default SerPredicate<T> and( SerPredicate<? super T> other )
    {
    Objects.requireNonNull( other );
    return ( t ) -> test( t ) && other.test( t );
    }

  default SerPredicate<T> negate()
    {
    return ( t ) -> !test( t );
    }

  default SerPredicate<T> or( SerPredicate<? super T> other )
    {
    Objects.requireNonNull( other );
    return ( t ) -> test( t ) || other.test( t );
    }
  }
