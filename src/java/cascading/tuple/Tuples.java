/*
 * Copyright (c) 2007-2008 Concurrent, Inc. All Rights Reserved.
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
      destination[ i ] = coerce( tuple, i, types[ i ] );

    return destination;
    }

  /**
   * Method coerce returns the value in the tuple at the given velue to the requested type.
   *
   * @param tuple
   * @param i
   * @param type
   * @return
   */
  public static Object coerce( Tuple tuple, int i, Class type )
    {
    if( type == Object.class )
      return tuple.get( i );
    else if( type == String.class )
      return tuple.getString( i );
    else if( type == Integer.class || type == int.class )
      return tuple.getInteger( i );
    else if( type == Long.class || type == long.class )
      return tuple.getLong( i );
    else if( type == Double.class || type == double.class )
      return tuple.getDouble( i );
    else if( type == Float.class || type == float.class )
      return tuple.getFloat( i );
    else if( type == Short.class || type == short.class )
      return tuple.getShort( i );
    else if( type == Boolean.class || type == boolean.class )
      return tuple.getBoolean( i );
    else if( type != null ) // make null if we don't know the type
      throw new OperationException( "could not coerce value, " + tuple.get( i ) + " to type: " + type.getName() );

    return null;
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
