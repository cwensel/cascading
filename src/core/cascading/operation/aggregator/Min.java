/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

/** Class Min is an {@link Aggregator} that returns the minimum value encountered in the current group. */
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

  protected boolean compare( Number lhs, Number rhs )
    {
    return lhs.doubleValue() > rhs.doubleValue();
    }

  protected double getInitialValue()
    {
    return Double.POSITIVE_INFINITY;
    }
  }
