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

import java.util.Collections;
import java.util.List;

import cascading.operation.OperationException;
import cascading.tuple.coerce.Coercions;
import cascading.tuple.type.CoercibleType;

/**
 * Class Tuples is a helper class providing common methods to manipulate {@link Tuple} and {@link TupleEntry} instances.
 *
 * @see Tuple
 * @see TupleEntry
 */
public class Tuples
  {
  /**
   * A utility function for use with Janino expressions to get around its lack of support for varargs.
   *
   * @param a of type Object
   * @return a new Tuple
   */
  public static Tuple tuple( Object a )
    {
    return new Tuple( a );
    }

  /**
   * A utility function for use with Janino expressions to get around its lack of support for varargs.
   *
   * @param a of type Object
   * @param b of type Object
   * @return a new Tuple
   */
  public static Tuple tuple( Object a, Object b )
    {
    return new Tuple( a, b );
    }

  /**
   * A utility function for use with Janino expressions to get around its lack of support for varargs.
   *
   * @param a of type Object
   * @param b of type Object
   * @param c of type Object
   * @return a new Tuple
   */
  public static Tuple tuple( Object a, Object b, Object c )
    {
    return new Tuple( a, b, c );
    }

  /**
   * A utility function for use with Janino expressions to get around its lack of support for varargs.
   *
   * @param a of type Object
   * @param b of type Object
   * @param c of type Object
   * @param d of type Object
   * @return a new Tuple
   */
  public static Tuple tuple( Object a, Object b, Object c, Object d )
    {
    return new Tuple( a, b, c, d );
    }

  /**
   * A utility function for use with Janino expressions to get around its lack of support for varargs.
   *
   * @param a of type Object
   * @param b of type Object
   * @param c of type Object
   * @param d of type Object
   * @param e of type Object
   * @return a new Tuple
   */
  public static Tuple tuple( Object a, Object b, Object c, Object d, Object e )
    {
    return new Tuple( a, b, c, d, e );
    }

  /**
   * A utility function for use with Janino expressions to get around its lack of support for varargs.
   *
   * @param a of type Object
   * @param b of type Object
   * @param c of type Object
   * @param d of type Object
   * @param e of type Object
   * @param f of type Object
   * @return a new Tuple
   */
  public static Tuple tuple( Object a, Object b, Object c, Object d, Object e, Object f )
    {
    return new Tuple( a, b, c, d, e, f );
    }

  /**
   * A utility function for use with Janino expressions to get around its lack of support for varargs.
   *
   * @param a of type Object
   * @param b of type Object
   * @param c of type Object
   * @param d of type Object
   * @param e of type Object
   * @param f of type Object
   * @param g of type Object
   * @return a new Tuple
   */
  public static Tuple tuple( Object a, Object b, Object c, Object d, Object e, Object f, Object g )
    {
    return new Tuple( a, b, c, d, e, f, g );
    }

  /**
   * A utility function for use with Janino expressions to get around its lack of support for varargs.
   *
   * @param a of type Object
   * @param b of type Object
   * @param c of type Object
   * @param d of type Object
   * @param e of type Object
   * @param f of type Object
   * @param g of type Object
   * @param h of type Object
   * @return a new Tuple
   */
  public static Tuple tuple( Object a, Object b, Object c, Object d, Object e, Object f, Object g, Object h )
    {
    return new Tuple( a, b, c, d, e, f, g, h );
    }

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
  @Deprecated
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
  @Deprecated
  public static Object[] asArray( Tuple tuple, Class[] types, Object[] destination )
    {
    if( tuple.size() != types.length )
      throw new OperationException( "number of input tuple values: " + tuple.size() + ", does not match number of coercion types: " + types.length );

    for( int i = 0; i < types.length; i++ )
      destination[ i ] = coerce( tuple, i, types[ i ] );

    return destination;
    }

  public static Object[] asArray( Tuple tuple, CoercibleType[] coercions, Class[] types, Object[] destination )
    {
    if( tuple.size() != types.length )
      throw new OperationException( "number of input tuple values: " + tuple.size() + ", does not match number of coercion types: " + types.length );

    for( int i = 0; i < types.length; i++ )
      destination[ i ] = coercions[ i ].coerce( tuple.getObject( i ), types[ i ] );

    return destination;
    }

  /**
   * Method frequency behaves the same as {@link Collections#frequency(java.util.Collection, Object)}.
   * <p/>
   * This method is a convenient way to test for all null values in a tuple.
   *
   * @param tuple of type Tuple
   * @param value of type Object
   * @return an int
   */
  public static int frequency( Tuple tuple, Object value )
    {
    return Collections.frequency( tuple.elements, value );
    }

  /**
   * Method frequency behaves the same as {@link Collections#frequency(java.util.Collection, Object)}.
   * <p/>
   * This method is a convenient way to test for all null values in a tuple.
   *
   * @param tupleEntry of type TupleEntry
   * @param value      of type Object
   * @return an int
   */
  public static int frequency( TupleEntry tupleEntry, Object value )
    {
    return Collections.frequency( tupleEntry.getTuple().elements, value );
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
  @Deprecated
  public static Object coerce( Tuple tuple, int pos, Class type )
    {
    Object value = tuple.getObject( pos );

    return coerce( value, type );
    }

  @Deprecated
  public static Object coerce( Object value, Class type )
    {
    return Coercions.coerce( value, type );
    }

  @Deprecated
  public static final String toString( Object value )
    {
    return Coercions.STRING.coerce( value );
    }

  @Deprecated
  public static final int toInteger( Object value )
    {
    return Coercions.INTEGER.coerce( value );
    }

  @Deprecated
  public static final long toLong( Object value )
    {
    return Coercions.LONG.coerce( value );
    }

  @Deprecated
  public static final double toDouble( Object value )
    {
    return Coercions.DOUBLE.coerce( value );
    }

  @Deprecated
  public static final float toFloat( Object value )
    {
    return Coercions.FLOAT.coerce( value );
    }

  @Deprecated
  public static final short toShort( Object value )
    {
    return Coercions.SHORT.coerce( value );
    }

  @Deprecated
  public static final boolean toBoolean( Object value )
    {
    return Coercions.BOOLEAN.coerce( value );
    }

  @Deprecated
  public static final Integer toIntegerObject( Object value )
    {
    return Coercions.INTEGER_OBJECT.coerce( value );
    }

  @Deprecated
  public static final Long toLongObject( Object value )
    {
    return Coercions.LONG_OBJECT.coerce( value );
    }

  @Deprecated
  public static final Double toDoubleObject( Object value )
    {
    return Coercions.DOUBLE_OBJECT.coerce( value );
    }

  @Deprecated
  public static final Float toFloatObject( Object value )
    {
    return Coercions.FLOAT_OBJECT.coerce( value );
    }

  @Deprecated
  public static final Short toShortObject( Object value )
    {
    return Coercions.SHORT_OBJECT.coerce( value );
    }

  @Deprecated
  public static final Boolean toBooleanObject( Object value )
    {
    return Coercions.BOOLEAN_OBJECT.coerce( value );
    }

  /**
   * Method coerce forces each element value in the given Tuple to the corresponding primitive type.
   *
   * @param tuple of type Tuple
   * @param types of type Class[]
   * @return Tuple
   */
  @Deprecated
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
  @Deprecated
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

      tupleEntry.setTuple( Tuple.size( result.size() ) );

      return result;
      }

    try
      {
      return extract( tupleEntry, selector );
      }
    catch( Exception exception )
      {
      throw new TupleException( "unable to select from: " + tupleEntry.getFields().printVerbose() + ", using selector: " + selector.printVerbose(), exception );
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
    return tupleEntry.tuple.extract( tupleEntry.getFields().getPos( selector, tupleEntry.getFields().size() ) );
    }

  public static Tuple nulledCopy( TupleEntry tupleEntry, Fields selector )
    {
    return tupleEntry.tuple.nulledCopy( tupleEntry.getFields().getPos( selector, tupleEntry.getFields().size() ) );
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
