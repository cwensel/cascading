/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

package cascading.tuple;

import java.util.List;

import cascading.operation.OperationException;

/**
 * Class Tuples is a helper class providing common methods to manipulate {@link Tuple} and {@link TupleEntry} instances.
 *
 * @see Tuple
 * @see TupleEntry
 */
public class Tuples
  {
  /**
   * Method asArray copies the elements of the given Tuple instance to the given Object array.
   *
   * @param tuple       of type Tuple
   * @param destination of type Object[]
   * @return Object[]
   */
  public static <T> T[] asArray( Tuple tuple, T[] destination )
    {
    if( tuple.size() != destination.length )
      throw new OperationException( "number of input tuple values: " + tuple.size() + ", does not match destination array size: " + destination.length );

    return tuple.elements( destination );
    }

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
    return asArray( tuple, types, new Object[ tuple.size() ] );
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
   * <p/>
   * If the given type is a primitive (long), and the tuple value is null, 0 is returned.
   * <p/>
   * If the type is an Object (Long), and the tuple value is null, null is returned.
   *
   * @param tuple of type Tuple
   * @param pos   of type int
   * @param type  of type Class
   * @return returns the value coerced
   */
  public static Object coerce( Tuple tuple, int pos, Class type )
    {
    Object value = tuple.getObject( pos );

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

    if( type == int.class )
      return toInteger( value );

    if( type == long.class )
      return toLong( value );

    if( type == double.class )
      return toDouble( value );

    if( type == float.class )
      return toFloat( value );

    if( type == short.class )
      return toShort( value );

    if( type == boolean.class )
      return toBoolean( value );

    if( type == Integer.class )
      return toIntegerObject( value );

    if( type == Long.class )
      return toLongObject( value );

    if( type == Double.class )
      return toDoubleObject( value );

    if( type == Float.class )
      return toFloatObject( value );

    if( type == Short.class )
      return toShortObject( value );

    if( type == Boolean.class )
      return toBooleanObject( value );

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

  public static final Integer toIntegerObject( Object value )
    {
    if( value instanceof Number )
      return ( (Number) value ).intValue();
    else if( value == null || value.toString().isEmpty() )
      return null;
    else
      return Integer.parseInt( value.toString() );
    }

  public static final Long toLongObject( Object value )
    {
    if( value instanceof Number )
      return ( (Number) value ).longValue();
    else if( value == null || value.toString().isEmpty() )
      return null;
    else
      return Long.parseLong( value.toString() );
    }

  public static final Double toDoubleObject( Object value )
    {
    if( value instanceof Number )
      return ( (Number) value ).doubleValue();
    else if( value == null || value.toString().isEmpty() )
      return null;
    else
      return Double.parseDouble( value.toString() );
    }

  public static final Float toFloatObject( Object value )
    {
    if( value instanceof Number )
      return ( (Number) value ).floatValue();
    else if( value == null || value.toString().isEmpty() )
      return null;
    else
      return Float.parseFloat( value.toString() );
    }

  public static final Short toShortObject( Object value )
    {
    if( value instanceof Number )
      return ( (Number) value ).shortValue();
    else if( value == null || value.toString().isEmpty() )
      return 0;
    else
      return Short.parseShort( value.toString() );
    }

  public static final Boolean toBooleanObject( Object value )
    {
    if( value instanceof Boolean )
      return (Boolean) value;
    else if( value == null || value.toString().isEmpty() )
      return null;
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
    return new Tuple( (Object[]) asArray( tuple, types, new Object[ types.length ] ) );
    }

  /**
   * Method coerce forces each element value in the given Tuple to the corresponding primitive type.
   * <p/>
   * This method expects the destination Tuple was created with the same size at the types array.
   *
   * @param tuple       of type Tuple
   * @param types       of type Class[]
   * @param destination of type Tuple
   * @return Tuple
   */
  public static Tuple coerce( Tuple tuple, Class[] types, Tuple destination )
    {
    if( tuple.size() != types.length )
      throw new OperationException( "number of input tuple values: " + tuple.size() + ", does not match number of coercion types: " + types.length );

    if( destination.size() != types.length )
      throw new OperationException( "number of destination tuple values: " + destination.size() + ", does not match number of coercion types: " + types.length );

    for( int i = 0; i < types.length; i++ )
      destination.set( i, coerce( tuple, i, types[ i ] ) );

    return destination;
    }

  /**
   * Method extractTuple returns a new Tuple based on the given selector. But sets the values of the
   * given TupleEntry to null.
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

  public static Tuple nulledCopy( TupleEntry tupleEntry, Fields selector )
    {
    return tupleEntry.tuple.nulledCopy( tupleEntry.fields.getPos( selector, tupleEntry.fields.size() ) );
    }

  public static Tuple nulledCopy( Fields declarator, Tuple tuple, Fields selector )
    {
    return tuple.nulledCopy( declarator.getPos( selector, declarator.size() ) );
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

  public static Tuple create( List<Object> arrayList )
    {
    return new Tuple( arrayList );
    }
  }
