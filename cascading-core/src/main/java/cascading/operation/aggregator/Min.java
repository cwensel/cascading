/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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
 * Class Min is an {@link Aggregator} that returns the minimum value encountered in the current group.
 * * <p/>
 * This class assumes the argument values is a {@link Number}, or a {@link String} representation of a number.
 * <p/>
 * See {@link MinValue} to find the min of any {@link Comparable} type.
 */
@Deprecated
public class Min extends ExtremaBase
  {
  /** Field FIELD_NAME */
  public static final String FIELD_NAME = "min";

  /** Constructs a new instance that returns the Min value encountered in the field name "min". */
  public Min()
    {
    super( 1, new Fields( FIELD_NAME ) );
    }

  /**
   * Constructs a new instance that returns the minimum value encountered in the given fieldDeclaration field name.
   *
   * @param fieldDeclaration of type Fields
   */
  @ConstructorProperties({"fieldDeclaration"})
  public Min( Fields fieldDeclaration )
    {
    super( 1, fieldDeclaration );
    }

  /**
   * Constructs a new instance that returns the minimum value encountered in the given fieldDeclaration field name.
   * Any argument matching an ignoredValue won't be compared.
   *
   * @param fieldDeclaration of type Fields
   * @param ignoreValues     of type Object...
   */
  @ConstructorProperties({"fieldDeclaration", "ignoreValues"})
  public Min( Fields fieldDeclaration, Object... ignoreValues )
    {
    super( fieldDeclaration, ignoreValues );
    }

  @Override
  protected boolean compare( Number lhs, Number rhs )
    {
    return lhs.doubleValue() > rhs.doubleValue();
    }

  @Override
  protected double getInitialValue()
    {
    return Double.POSITIVE_INFINITY;
    }
  }
