/*
 * Copyright (c) 2007-20010 Concurrent, Inc. All Rights Reserved.
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
 * Class Tuples is a helper class providing common methods to manipulate {@link Tuple} and {@link TupleEntry} instances.
 *
 * @see Tuple
 * @see TupleEntry
 */
public class Tuples
  {
  /** A constant empty Tuple instance. This instance is immutable. */
  public static final Tuple NULL = asUnmodifiable( new Tuple() );

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
    Object value = tuple.get( pos );

    return coerce( value, type );
    }

  public static Object coerce( Object value, Class type )
    {
    if( value != null && type == value.getClass() )
      return value;

    if( type == Object.class )
      return value;

    if( type == String.class )
      return toString( value );

    if( type == Integer.class || type == int.class )
      return toInteger( value );

    if( type == Long.class || type == long.class )
      return toLong( value );

    if( type == Double.class || type == double.class )
      return toDouble( value );

    if( type == Float.class || type == float.class )
      return toFloat( value );

    if( type == Short.class || type == short.class )
      return toShort( value );

    if( type == Boolean.class || type == boolean.class )
      return toBoolean( value );

    if( type != null )
      throw new OperationException( "could not coerce value, " + value + " to type: " + type.getName() );

    return null;
    }

  public static final String toString( Object value )
    {
    if( value == null )
      return null;

    return value.toString();
    }

  public static final int toInteger( Object value )
    {
    if( value instanceof Number )
      return ( (Number) value ).intValue();
    else if( value == null )
      return 0;
    else
      return Integer.parseInt( value.toString() );
    }

  public static final long toLong( Object value )
    {
    if( value instanceof Number )
      return ( (Number) value ).longValue();
    else if( value == null )
      return 0;
    else
      return Long.parseLong( value.toString() );
    }

  public static final double toDouble( Object value )
    {
    if( value instanceof Number )
      return ( (Number) value ).doubleValue();
    else if( value == null )
      return 0;
    else
      return Double.parseDouble( value.toString() );
    }

  public static final float toFloat( Object value )
    {
    if( value instanceof Number )
      return ( (Number) value ).floatValue();
    else if( value == null )
      return 0;
    else
      return Float.parseFloat( value.toString() );
    }

  public static final short toShort( Object value )
    {
    if( value instanceof Number )
      return ( (Number) value ).shortValue();
    else if( value == null )
      return 0;
    else
      return Short.parseShort( value.toString() );
    }

  public static final boolean toBoolean( Object value )
    {
    if( value instanceof Boolean )
      return ( (Boolean) value ).booleanValue();
    else if( value == null )
      return false;
    else
      return Boolean.parseBoolean( value.toString() );
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
    return new Tuple( (Object[]) asArray( tuple, types, new Object[types.length] ) );
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

  /**
   * Method asUnmodifiable marks the given Tuple instance as unmodifiable.
   *
   * @param tuple of type Tuple
   * @return Tuple
   */
  public static Tuple asUnmodifiable( Tuple tuple )
    {
    tuple.isUnmodifiable = true;

    return tuple;
    }

  /**
   * Method asModifiable marks the given Tuple instance as modifiable.
   *
   * @param tuple of type Tuple
   * @return Tuple
   */
  public static Tuple asModifiable( Tuple tuple )
    {
    tuple.isUnmodifiable = false;

    return tuple;
    }
  }
