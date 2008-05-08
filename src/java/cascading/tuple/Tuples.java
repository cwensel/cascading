/*
 * Copyright (c) 2007-2008 Chris K Wensel. All Rights Reserved.
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

package cascading.tuple;

import cascading.operation.OperationException;

/**
 *
 */
public class Tuples
  {
  /**
   * Method asArray convert the given {@link Tuple} instance into an Object[]. The given Class[] array
   * denotes the types each tuple element value should be coerced into.
   * <p/>
   * Coercion types are Object, String, Integer, Long, Float, Double, Short, and Boolean.
   * <p/>
   * If all Tuple element values are null, they will remain null for String and Object, but become zero for the numeric types.
   * <p/>
   * The string value 'true' can be converted to the boolean true.
   *
   * @param tuple of type Tuple
   * @param types of type Class[]
   * @return Object[]
   */
  public static Object[] asArray( Tuple tuple, Class[] types )
    {
    return asArray( tuple, types, new Object[tuple.size()] );
    }

  /**
   * Method asArray convert the given {@link Tuple} instance into an Object[]. The given Class[] array
   * denotes the types each tuple element value should be coerced into.
   *
   * @param tuple       of type Tuple
   * @param types       of type Class[]
   * @param destination of type Object[]
   * @return Object[]
   */
  public static Object[] asArray( Tuple tuple, Class[] types, Object[] destination )
    {
    if( tuple.size() != types.length )
      throw new OperationException( "number of input tuple values: " + tuple.size() + ", does not match number of coercion types: " + types.length );

    for( int i = 0; i < types.length; i++ )
      {
      Class type = types[ i ];

      if( type == Object.class )
        destination[ i ] = tuple.get( i );
      else if( type == String.class )
        destination[ i ] = tuple.getString( i );
      else if( type == Integer.class || type == int.class )
        destination[ i ] = tuple.getInteger( i );
      else if( type == Long.class || type == long.class )
        destination[ i ] = tuple.getLong( i );
      else if( type == Double.class || type == double.class )
        destination[ i ] = tuple.getDouble( i );
      else if( type == Float.class || type == float.class )
        destination[ i ] = tuple.getFloat( i );
      else if( type == Short.class || type == short.class )
        destination[ i ] = tuple.getShort( i );
      else if( type == Boolean.class || type == boolean.class )
        destination[ i ] = tuple.getBoolean( i );
      else if( type != null ) // make null if we don't know the type
        throw new OperationException( "could not coerce value, " + tuple.get( i ) + " to type: " + type.getName() );
      }

    return destination;
    }

  /**
   * Method coerce forces each element value in the given Tuple to the corresponding primitive type.
   *
   * @param tuple of type Tuple
   * @param types of type Class[]
   * @return Tuple
   */
  public static Tuple coerce( Tuple tuple, Class[] types )
    {
    return new Tuple( (Comparable[]) asArray( tuple, types, new Comparable[types.length] ) );
    }
  }
