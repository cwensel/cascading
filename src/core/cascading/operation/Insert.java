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

package cascading.operation;

import java.beans.ConstructorProperties;

import cascading.flow.FlowProcess;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/** Class Insert adds literal values to the Tuple stream. */
public class Insert extends BaseOperation implements Function
  {
  /** Field values */
  private final Tuple values;

  /**
   * Constructor Insert creates a new Insert instance with the given fields and values.
   *
   * @param fieldDeclaration of type Fields
   * @param values           of type Object...
   */
  @ConstructorProperties({"fieldDeclaration", "values"})
  public Insert( Fields fieldDeclaration, Object... values )
    {
    super( 0, fieldDeclaration );
    this.values = new Tuple( values );

    if( !fieldDeclaration.isSubstitution() && fieldDeclaration.size() != values.length )
      throw new IllegalArgumentException( "fieldDeclaration must be the same size as the given values" );
    }

  /** @see Function#operate(cascading.flow.FlowProcess, FunctionCall) */
  public void operate( FlowProcess flowProcess, FunctionCall functionCall )
    {
    functionCall.getOutputCollector().add( new Tuple( values ) );
    }
  }
