/*
 * Copyright (c) 2007-2008 Vinculum Technologies, Inc. All Rights Reserved.
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

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleCollector;
import cascading.tuple.TupleEntry;

/**
 * The Identity function simply passes incoming arguments back out again. Optionally argument fields can be renamed, and/or
 * coerced into specfic types.
 */
public class Identity extends Operation implements Function
  {
  /** Field types */
  private Class[] types = null;

  /**
   * Constructor Identity creates a new Identity instance that will pass the argument values to its output. Use this
   * constructor for a simple copy Pipe.
   */
  public Identity()
    {
    super( Fields.ALL );
    }

  /**
   * Constructor Identity creates a new Identity instance that will coerce the values to the give types.
   *
   * @param types of type Class...
   */
  public Identity( Class... types )
    {
    super( Fields.ALL );
    this.types = types;
    }

  /**
   * Constructor Identity creates a new Identity instance that will rename the argument fields to the given
   * fieldDeclaration.
   *
   * @param fieldDeclaration of type Fields
   */
  public Identity( Fields fieldDeclaration )
    {
    super( fieldDeclaration ); // don't need to set size, default is ANY
    }

  protected Identity( int numArgs, Fields fieldDeclaration )
    {
    super( numArgs, fieldDeclaration );
    }

  /**
   * Constructor Identity creates a new Identity instance that will rename the argument fields to the given
   * fieldDeclaration, and coerce the values to the give types.
   *
   * @param fieldDeclaration of type Fields
   * @param types            of type Class...
   */
  public Identity( Fields fieldDeclaration, Class... types )
    {
    super( fieldDeclaration );
    this.types = types;

    if( !fieldDeclaration.isSubstitution() && fieldDeclaration.size() != types.length )
      throw new IllegalArgumentException( "fieldDeclaration and types must be the same size" );
    }

  public void operate( TupleEntry input, TupleCollector outputCollector )
    {
    if( types == null || types.length == 0 )
      {
      outputCollector.add( input );
      return;
      }

    if( input.size() != types.length )
      throw new OperationException( "number of input tuple values: " + input.size() + ", does not match number of coercion types: " + types.length );

    Tuple result = new Tuple();

    for( int i = 0; i < types.length; i++ )
      {
      Class type = types[ i ];

      if( type == Object.class )
        result.add( input.get( i ) );
      else if( type == String.class )
        result.add( input.getTuple().getString( i ) );
      else if( type == Float.class )
        result.add( input.getTuple().getFloat( i ) );
      else if( type == Double.class )
        result.add( input.getTuple().getDouble( i ) );
      else if( type == Integer.class )
        result.add( input.getTuple().getInteger( i ) );
      else if( type == Long.class )
        result.add( input.getTuple().getLong( i ) );
      else if( type == Short.class )
        result.add( input.getTuple().getShort( i ) );
      else
        throw new OperationException( "could not coerce value, " + input.get( i ) + " to type: " + type.getName() );
      }

    outputCollector.add( result );
    }
  }
