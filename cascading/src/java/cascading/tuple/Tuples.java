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

package cascading.tuple;

/**
 *
 */
public class Tuples
  {
  /**
   * Method asArray convert the given {@link Tuple} instance into an Object[]. The given Class[] array
   * denotes the types each tuple element value should be coerced into.
   *
   * @param tuple of type Tuple
   * @param types of type Class[]
   * @return Object[]
   */
  public static Object[] asArray( Tuple tuple, Class[] types )
    {
    Object[] results = new Object[tuple.size()];

    for( int i = 0; i < types.length; i++ )
      {
      Class type = types[ i ];

      if( type == String.class )
        results[ i ] = tuple.getString( i );
      else if( type == Integer.class || type == int.class )
        results[ i ] = tuple.getInteger( i );
      else if( type == Long.class || type == long.class )
        results[ i ] = tuple.getLong( i );
      else if( type == Double.class || type == double.class )
        results[ i ] = tuple.getDouble( i );
      else if( type == Float.class || type == float.class )
        results[ i ] = tuple.getFloat( i );
      }

    return results;
    }
  }
