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

/** Class Tuples is a helper class providing common methods to manipulate {@link Tuple} instances. */
public class Tuples
  {
  /** A constant empty Tuple instance. Immutability is not currently enforced. Use with caution. */
  public static final Tuple NULL = new Tuple();

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
   * Method coerce returns the value in the tuple at the given position to the requested type.
   *
   * @param tuple of type Tuple
   * @param pos   of type int
   * @param type  of type Class
   * @return returns the value coerced
   */
  public static Object coerce( Tuple tuple, int pos, Class type )
    {
    if( type == Object.class )
      return tuple.get( pos );
    else if( type == String.class )
      return tuple.getString( pos );
    else if( type == Integer.class || type == int.class )
        return tuple.getInteger( pos );
      else if( type == Long.class || type == long.class )
          return tuple.getLong( pos );
        else if( type == Double.class || type == double.class )
            return tuple.getDouble( pos );
          else if( type == Float.class || type == float.class )
              return tuple.getFloat( pos );
            else if( type == Short.class || type == short.class )
                return tuple.getShort( pos );
              else if( type == Boolean.class || type == boolean.class )
                  return tuple.getBoolean( pos );
                else if( type != null )
                    throw new OperationException( "could not coerce value, " + tuple.get( pos ) + " to type: " + type.getName() );

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

  /**
   * Method extractTuple returns a new Tuple based on the given selector. But sets the values of this entries Tuple to null.
   *
   * @param tupleEntry of type TupleEntry
   * @param selector   of type Fields
   * @return Tuple
   */
  public static Tuple extractTuple( TupleEntry tupleEntry, Fields selector )
    {
    if( selector == null || selector.isAll() )
      {
      Tuple result = tupleEntry.tuple;

      tupleEntry.tuple = Tuple.size( result.size() );

      return result;
      }

    try
      {
      return extract( tupleEntry, selector );
      }
    catch( Exception exception )
      {
      throw new TupleException( "unable to select from: " + tupleEntry.fields.print() + ", using selector: " + selector.print(), exception );
      }
    }

  /**
   * Method extract creates a new Tuple from the given selector, but sets the values in the current tuple to null.
   *
   * @param tupleEntry of type TupleEntry
   * @param selector   of type Fields
   * @return Tuple
   */
  public static Tuple extract( TupleEntry tupleEntry, Fields selector )
    {
    return tupleEntry.tuple.extract( tupleEntry.fields.getPos( selector, tupleEntry.fields.size() ) );
    }

  public static Tuple setOnEmpty( TupleEntry baseEntry, TupleEntry valuesEntry )
    {
    Tuple emptyTuple = Tuple.size( baseEntry.getFields().size() );

    emptyTuple.set( baseEntry.getFields(), valuesEntry.getFields(), valuesEntry.getTuple() );

    return emptyTuple;
    }

  }
