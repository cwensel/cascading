/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

package cascading.operation.aggregator;

import java.beans.ConstructorProperties;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;

/**
 * Class Max is an {@link Aggregator} that returns the maximum value encountered in the current group.
 * <p/>
 * This class assumes the argument values is a {@link Number}, or a {@link String} representation of a number.
 * <p/>
 * See {@link MaxValue} to find the max of any {@link Comparable} type.
 */
@Deprecated
public class Max extends ExtremaBase
  {
  /** Field FIELD_NAME */
  public static final String FIELD_NAME = "max";

  /** Constructs a new instance that returns the maximum value encountered in the field name "max". */
  public Max()
    {
    super( 1, new Fields( FIELD_NAME ) );
    }

  /**
   * Constructs a new instance that returns the maximum value encountered in the given fieldDeclaration field name.
   *
   * @param fieldDeclaration of type Fields
   */
  @ConstructorProperties({"fieldDeclaration"})
  public Max( Fields fieldDeclaration )
    {
    super( 1, fieldDeclaration );
    }

  /**
   * Constructs a new instance that returns the maximum value encountered in the given fieldDeclaration field name.
   * Any argument matching an ignoredValue won't be compared.
   *
   * @param fieldDeclaration of type Fields
   * @param ignoreValues     of type Object...
   */
  @ConstructorProperties({"fieldDeclaration", "ignoreValues"})
  public Max( Fields fieldDeclaration, Object... ignoreValues )
    {
    super( fieldDeclaration, ignoreValues );
    }

  @Override
  protected boolean compare( Number lhs, Number rhs )
    {
    return lhs.doubleValue() < rhs.doubleValue();
    }

  @Override
  protected double getInitialValue()
    {
    return Double.NEGATIVE_INFINITY;
    }
  }
