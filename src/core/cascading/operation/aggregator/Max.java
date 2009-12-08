/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
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

package cascading.operation.aggregator;

import java.beans.ConstructorProperties;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;

/** Class Max is an {@link Aggregator} that returns the maximum value encountered in the current group. */
public class Max extends ExtremaBase
  {
  /** Field FIELD_NAME */
  public static final String FIELD_NAME = "max";

  /** Constructs a new instance that returns the maximum value encoutered in the field name "max". */
  public Max()
    {
    super( 1, new Fields( FIELD_NAME ) );
    }

  /**
   * Constructs a new instance that returns the maximum value encoutered in the given fieldDeclaration field name.
   *
   * @param fieldDeclaration of type Fields
   */
  @ConstructorProperties({"fieldDeclaration"})
  public Max( Fields fieldDeclaration )
    {
    super( 1, fieldDeclaration );
    }

  /**
   * Constructs a new instance that returns the maximum value encoutered in the given fieldDeclaration field name.
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

  protected boolean compare( Number lhs, Number rhs )
    {
    return lhs.doubleValue() < rhs.doubleValue();
    }

  protected double getInitialValue()
    {
    return Double.NEGATIVE_INFINITY;
    }
  }
