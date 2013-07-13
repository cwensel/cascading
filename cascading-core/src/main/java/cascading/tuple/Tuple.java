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

import java.beans.ConstructorProperties;
import java.io.Serializable;
import java.io.StringReader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

import cascading.tuple.coerce.Coercions;
import cascading.tuple.type.CoercibleType;
import cascading.util.Util;

/**
 * A Tuple represents a set of values. Consider a Tuple the same as a database record where every value is a column in
 * that table.
 * <p/>
 * A "tuple stream" is a set of Tuple instances passed consecutively through a Pipe assembly.
 * <p/>
 * Tuples work in tandem with {@link Fields} and the {@link TupleEntry} classes. A TupleEntry holds an instance of
 * Fields and a Tuple. It allows a tuple to be accessed by its field names, and will help maintain consistent types
 * if any are given on the Fields instance. That is, if a field is specified at an Integer, calling {@link #set(int, Object)}
 * with a String will force the String to be coerced into a Integer instance.
 * <p/>
 * For managing custom types, see the {@link CoercibleType} interface which extends {@link Type}.
 * <p/>
 * Tuple instances created by user code, by default, are mutable (or modifiable).
 * Tuple instances created by the system are immutable (or unmodifiable, tested by calling {@link #isUnmodifiable()}).
 * <p/>
 * For example tuples returned by
 * {@link cascading.operation.FunctionCall#getArguments()}, will always be unmodifiable. Thus they must be copied
 * if they will be changed by user code or cached in the local context. See the Tuple copy constructor, or {@code *Copy()} methods
 * on {@link TupleEntry}.
 * <p/>
 * Because a Tuple can hold any Object type, it is suitable for storing custom types. But all custom types
 * must have a serialization support per the underlying framework.
 * <p/>
 * For Hadoop, a {@link org.apache.hadoop.io.serializer.Serialization} implementation
 * must be registered with Hadoop. For further performance improvements, see the
 * {@link cascading.tuple.hadoop.SerializationToken} Java annotation.
 *
 * @see org.apache.hadoop.io.serializer.Serialization
 * @see cascading.tuple.hadoop.SerializationToken
 */
public class Tuple implements Comparable<Object>, Iterable<Object>, Serializable
  {
  /** A constant empty Tuple instance. This instance is immutable. */
  public static final Tuple NULL = Tuples.asUnmodifiable( new Tuple() );

  /** Field printDelim */
  private final static String printDelim = "\t";

  /** Field isUnmodifiable */
  protected boolean isUnmodifiable = false;
  /** Field elements */
  protected List<Object> elements;

  /**
   * Method size returns a new Tuple instance of the given size with nulls as its element values.
   *
   * @param size of type int
   * @return Tuple
   */
  public static Tuple size( int size )
    {
    return size( size, null );
    }

  /**
   * Method size returns a new Tuple instance of the given size with the given Comparable as its element values.
   *
   * @param size  of type int
   * @param value of type Comparable
   * @return Tuple
   */
  public static Tuple size( int size, Comparable value )
    {
    Tuple result = new Tuple( new ArrayList<Object>( size ) );

    for( int i = 0; i < size; i++ )
      result.add( value );

    return result;
    }

  /**
   * Method parse will parse the {@link #print()} String representation of a Tuple instance and return a new Tuple instance.
   * <p/>
   * This method has been deprecated as it doesn't properly handle nulls, and any types other than primitive types.
   *
   * @param string of type String
   * @return Tuple
   * @deprecated
   */
  @Deprecated
  public static Tuple parse( String string )
    {
    if( string == null || string.length() == 0 )
      return null;

    string = string.replaceAll( "^ *\\[*", "" );
    string = string.replaceAll( "\\]* *$", "" );

    Scanner scanner = new Scanner( new StringReader( string ) );
    scanner.useDelimiter( "(' *, *')|(^ *')|(' *$)" );

    Tuple result = new Tuple();

    while( scanner.hasNext() )
      {
      if( scanner.hasNextInt() )
        result.add( scanner.nextInt() );
      else if( scanner.hasNextDouble() )
        result.add( scanner.nextDouble() );
      else
        result.add( scanner.next() );
      }

    scanner.close();

    return result;
    }

  /**
   * Returns a reference to the private elements of the given Tuple.
   * <p/>
   * This method is for internal use and is subject to change scope in a future release.
   *
   * @param tuple of type Tuple
   * @return List<Comparable>
   */
  public static List<Object> elements( Tuple tuple )
    {
    return tuple.elements;
    }

  protected Tuple( List<Object> elements )
    {
    this.elements = elements;
    }

  /** Constructor Tuple creates a new Tuple instance. */
  public Tuple()
    {
    this( new ArrayList<Object>() );
    }

  /**
   * Copy constructor. Does not nest the given Tuple instance within this new instance. Use {@link #add(Object)}.
   *
   * @param tuple of type Tuple
   */
  @ConstructorProperties({"tuple"})
  public Tuple( Tuple tuple )
    {
    this( new ArrayList<Object>( tuple.elements ) );
    }

  /**
   * Constructor Tuple creates a new Tuple instance with all the given values.
   *
   * @param values of type Object...
   */
  @ConstructorProperties({"values"})
  public Tuple( Object... values )
    {
    this( new ArrayList<Object>( values.length ) );
    Collections.addAll( elements, values );
    }

  /**
   * Method isUnmodifiable returns true if this Tuple instance is unmodifiable.
   * <p/>
   * "Unmodifiable" tuples are generally owned by the system and cannot be changed, nor should they be cached
   * as the internal values may change.
   *
   * @return boolean
   */
  public boolean isUnmodifiable()
    {
    return isUnmodifiable;
    }

  /**
   * Method get returns the element at the given position i.
   * <p/>
   * This method assumes the element implements {@link Comparable} in order to maintain backwards compatibility. See
   * {@link #getObject(int)} for an alternative.
   * <p/>
   * This method is deprecated, use {@link #getObject(int)} instead.
   *
   * @param pos of type int
   * @return Comparable
   */
  @Deprecated
  public Comparable get( int pos )
    {
    return (Comparable) elements.get( pos );
    }

  /**
   * Method get returns the element at the given position.
   * <p/>
   * This method will perform no coercion on the element.
   *
   * @param pos of type int
   * @return Object
   */
  public Object getObject( int pos )
    {
    return elements.get( pos );
    }

  /**
   * Method getString returns the element at the given position as a String.
   *
   * @param pos of type int
   * @return String
   */
  public String getString( int pos )
    {
    return Coercions.STRING.coerce( getObject( pos ) );
    }

  /**
   * Method getFloat returns the element at the given position as a float. Zero if null.
   *
   * @param pos of type int
   * @return float
   */
  public float getFloat( int pos )
    {
    return Coercions.FLOAT.coerce( getObject( pos ) );
    }

  /**
   * Method getDouble returns the element at the given position as a double. Zero if null.
   *
   * @param pos of type int
   * @return double
   */
  public double getDouble( int pos )
    {
    return Coercions.DOUBLE.coerce( getObject( pos ) );
    }

  /**
   * Method getInteger returns the element at the given position as an int. Zero if null.
   *
   * @param pos of type int
   * @return int
   */
  public int getInteger( int pos )
    {
    return Coercions.INTEGER.coerce( getObject( pos ) );
    }

  /**
   * Method getLong returns the element at the given position as an long. Zero if null.
   *
   * @param pos of type int
   * @return long
   */
  public long getLong( int pos )
    {
    return Coercions.LONG.coerce( getObject( pos ) );
    }

  /**
   * Method getShort returns the element at the given position as an short. Zero if null.
   *
   * @param pos of type int
   * @return long
   */
  public short getShort( int pos )
    {
    return Coercions.SHORT.coerce( getObject( pos ) );
    }

  /**
   * Method getBoolean returns the element at the given position as a boolean. If the value is (case ignored) the
   * string 'true', a {@code true} value will be returned. {@code false} if null.
   *
   * @param pos of type int
   * @return boolean
   */
  public boolean getBoolean( int pos )
    {
    return Coercions.BOOLEAN.coerce( getObject( pos ) );
    }

  /**
   * Method get will return a new Tuple instance populated with element values from the given array of positions.
   *
   * @param pos of type int[]
   * @return Tuple
   */
  public Tuple get( int[] pos )
    {
    if( pos == null || pos.length == 0 )
      return new Tuple( this );

    Tuple results = new Tuple();

    for( int i : pos )
      results.add( elements.get( i ) );

    return results;
    }

  /**
   * Method get returns a new Tuple populated with only those values whose field names are specified in the given
   * selector. The declarator Fields instance declares the fields and positions in the current Tuple instance.
   *
   * @param declarator of type Fields
   * @param selector   of type Fields
   * @return Tuple
   */
  public Tuple get( Fields declarator, Fields selector )
    {
    try
      {
      return get( getPos( declarator, selector ) );
      }
    catch( Exception exception )
      {
      throw new TupleException( "unable to select from: " + declarator.print() + ", using selector: " + selector.print(), exception );
      }
    }

  public int[] getPos( Fields declarator, Fields selector )
    {
    if( !declarator.isUnknown() && elements.size() != declarator.size() )
      throw new TupleException( "field declaration: " + declarator.print() + ", does not match tuple: " + print() );

    return declarator.getPos( selector, size() );
    }

  /**
   * Method is the inverse of {@link #remove(int[])}.
   *
   * @param pos of type int[]
   * @return Tuple
   */
  public Tuple leave( int[] pos )
    {
    verifyModifiable();

    Tuple results = remove( pos );

    List<Object> temp = results.elements;
    results.elements = this.elements;
    this.elements = temp;

    return results;
    }

  /** Method clear empties this Tuple instance. A subsequent call to {@link #size()} will return zero ({@code 0}). */
  public void clear()
    {
    verifyModifiable();

    elements.clear();
    }

  /**
   * Method add adds a new element value to this instance.
   *
   * @param value of type Comparable
   */
  public void add( Comparable value )
    {
    add( (Object) value );
    }

  /**
   * Method add adds a new element value to this instance.
   *
   * @param value of type Object
   */
  public void add( Object value )
    {
    verifyModifiable();

    elements.add( value );
    }

  /**
   * Method addBoolean adds a new element value to this instance.
   *
   * @param value of type boolean
   */
  public void addBoolean( boolean value )
    {
    verifyModifiable();

    elements.add( value );
    }

  /**
   * Method addShort adds a new element value to this instance.
   *
   * @param value of type short
   */
  public void addShort( short value )
    {
    verifyModifiable();

    elements.add( value );
    }

  /**
   * Method addInteger adds a new element value to this instance.
   *
   * @param value of type int
   */
  public void addInteger( int value )
    {
    verifyModifiable();

    elements.add( value );
    }

  /**
   * Method addLong adds a new element value to this instance.
   *
   * @param value of type long
   */
  public void addLong( long value )
    {
    verifyModifiable();

    elements.add( value );
    }

  /**
   * Method addFloat adds a new element value to this instance.
   *
   * @param value of type float
   */
  public void addFloat( float value )
    {
    verifyModifiable();

    elements.add( value );
    }

  /**
   * Method addDouble adds a new element value to this instance.
   *
   * @param value of type double
   */
  public void addDouble( double value )
    {
    verifyModifiable();

    elements.add( value );
    }

  /**
   * Method addString adds a new element value to this instance.
   *
   * @param value of type String
   */
  public void addString( String value )
    {
    verifyModifiable();

    elements.add( value );
    }

  /**
   * Method addAll adds all given values to this instance.
   *
   * @param values of type Object...
   */
  public void addAll( Object... values )
    {
    verifyModifiable();

    if( values.length == 1 && values[ 0 ] instanceof Tuple )
      addAll( (Tuple) values[ 0 ] );
    else
      Collections.addAll( elements, values );
    }

  /**
   * Method addAll adds all the element values of the given Tuple instance to this instance.
   *
   * @param tuple of type Tuple
   */
  public void addAll( Tuple tuple )
    {
    verifyModifiable();

    if( tuple != null )
      elements.addAll( tuple.elements );
    }

  /**
   * Method setAll sets each element value of the given Tuple instance into the corresponding
   * position of this instance.
   *
   * @param tuple of type Tuple
   */
  public void setAll( Tuple tuple )
    {
    verifyModifiable();

    if( tuple == null )
      return;

    for( int i = 0; i < tuple.elements.size(); i++ )
      internalSet( i, tuple.elements.get( i ) );
    }

  /**
   * Method setAll sets each element value of the given Tuple instances into the corresponding
   * position of this instance.
   * <p/>
   * All given tuple instances after the first will be offset by the length of the prior tuple instances.
   *
   * @param tuples of type Tuple[]
   */
  public void setAll( Tuple... tuples )
    {
    verifyModifiable();

    if( tuples.length == 0 )
      return;

    int pos = 0;
    for( int i = 0; i < tuples.length; i++ )
      {
      Tuple tuple = tuples[ i ];

      if( tuple == null ) // being defensive
        continue;

      for( int j = 0; j < tuple.elements.size(); j++ )
        internalSet( pos++, tuple.elements.get( j ) );
      }
    }

  /**
   * Method set sets the given value to the given index position in this instance.
   *
   * @param index of type int
   * @param value of type Object
   */
  public void set( int index, Object value )
    {
    verifyModifiable();

    internalSet( index, value );
    }

  /**
   * Method setBoolean sets the given value to the given index position in this instance.
   *
   * @param index of type int
   * @param value of type boolean
   */
  public void setBoolean( int index, boolean value )
    {
    verifyModifiable();

    internalSet( index, value );
    }

  /**
   * Method setShort sets the given value to the given index position in this instance.
   *
   * @param index of type int
   * @param value of type short
   */
  public void setShort( int index, short value )
    {
    verifyModifiable();

    internalSet( index, value );
    }

  /**
   * Method setInteger sets the given value to the given index position in this instance.
   *
   * @param index of type int
   * @param value of type int
   */
  public void setInteger( int index, int value )
    {
    verifyModifiable();

    internalSet( index, value );
    }

  /**
   * Method setLong sets the given value to the given index position in this instance.
   *
   * @param index of type int
   * @param value of type long
   */
  public void setLong( int index, long value )
    {
    verifyModifiable();

    internalSet( index, value );
    }

  /**
   * Method setFloat sets the given value to the given index position in this instance.
   *
   * @param index of type int
   * @param value of type float
   */
  public void setFloat( int index, float value )
    {
    verifyModifiable();

    internalSet( index, value );
    }

  /**
   * Method setDouble sets the given value to the given index position in this instance.
   *
   * @param index of type int
   * @param value of type double
   */
  public void setDouble( int index, double value )
    {
    verifyModifiable();

    internalSet( index, value );
    }

  /**
   * Method setString sets the given value to the given index position in this instance.
   *
   * @param index of type int
   * @param value of type String
   */
  public void setString( int index, String value )
    {
    verifyModifiable();

    internalSet( index, value );
    }

  protected final void internalSet( int index, Object value )
    {
    try
      {
      elements.set( index, value );
      }
    catch( IndexOutOfBoundsException exception )
      {
      if( elements.size() != 0 )
        throw new TupleException( "failed to set a value beyond the end of the tuple elements array, size: " + size() + " , index: " + index );
      else
        throw new TupleException( "failed to set a value, tuple may not be initialized with values, is zero length" );
      }
    }

  /**
   * Method put places the values of the given tuple into the positions specified by the fields argument. The declarator
   * Fields value declares the fields in this Tuple instance.
   *
   * @param declarator of type Fields
   * @param fields     of type Fields
   * @param tuple      of type Tuple
   */
  public void put( Fields declarator, Fields fields, Tuple tuple )
    {
    verifyModifiable();

    int[] pos = getPos( declarator, fields );

    for( int i = 0; i < pos.length; i++ )
      internalSet( pos[ i ], tuple.getObject( i ) );
    }

  /**
   * Method remove removes the values specified by the given pos array and returns a new Tuple containing the
   * removed values.
   *
   * @param pos of type int[]
   * @return Tuple
   */
  public Tuple remove( int[] pos )
    {
    verifyModifiable();

    // calculate offsets to apply when removing values from elements
    int offset[] = new int[ pos.length ];

    for( int i = 0; i < pos.length; i++ )
      {
      offset[ i ] = 0;

      for( int j = 0; j < i; j++ )
        {
        if( pos[ j ] < pos[ i ] )
          offset[ i ]++;
        }
      }

    Tuple results = new Tuple();

    for( int i = 0; i < pos.length; i++ )
      results.add( elements.remove( pos[ i ] - offset[ i ] ) );

    return results;
    }

  /**
   * Method remove removes the values specified by the given selector. The declarator declares the fields in this instance.
   *
   * @param declarator of type Fields
   * @param selector   of type Fields
   * @return Tuple
   */
  public Tuple remove( Fields declarator, Fields selector )
    {
    return remove( getPos( declarator, selector ) );
    }

  /**
   * Creates a new Tuple from the given positions, but sets the values in the current tuple to null.
   *
   * @param pos of type int[]
   * @return Tuple
   */
  Tuple extract( int[] pos )
    {
    Tuple results = new Tuple();

    for( int i : pos )
      results.add( elements.set( i, null ) );

    return results;
    }

  Tuple nulledCopy( int[] pos )
    {
    if( pos == null )
      return size( size() );

    Tuple results = new Tuple( this );

    for( int i : pos )
      results.set( i, null );

    return results;
    }

  /**
   * Sets the values in the given positions to the values from the given Tuple.
   *
   * @param pos   of type int[]
   * @param tuple of type Tuple
   */
  void set( int[] pos, Tuple tuple )
    {
    verifyModifiable();

    if( pos.length != tuple.size() )
      throw new TupleException( "given tuple not same size as position array: " + pos.length + ", tuple: " + tuple.print() );

    int count = 0;
    for( int i : pos )
      elements.set( i, tuple.elements.get( count++ ) );
    }

  private void set( int[] pos, Type[] types, Tuple tuple, CoercibleType[] coercions )
    {
    verifyModifiable();

    if( pos.length != tuple.size() )
      throw new TupleException( "given tuple not same size as position array: " + pos.length + ", tuple: " + tuple.print() );

    int count = 0;

    for( int i : pos )
      {
      Object element = tuple.elements.get( count );
      Type type = types[ count++ ];
      element = coercions[ i ].coerce( element, type );

      elements.set( i, element );
      }
    }

  /**
   * Method set sets the values in the given selector positions to the values from the given Tuple.
   *
   * @param declarator of type Fields
   * @param selector   of type Fields
   * @param tuple      of type Tuple
   */
  public void set( Fields declarator, Fields selector, Tuple tuple )
    {
    try
      {
      set( declarator.getPos( selector ), tuple );
      }
    catch( Exception exception )
      {
      throw new TupleException( "unable to set into: " + declarator.print() + ", using selector: " + selector.print(), exception );
      }
    }

  protected void set( Fields declarator, Fields selector, Tuple tuple, CoercibleType[] coercions )
    {
    try
      {
      set( declarator.getPos( selector ), declarator.getTypes(), tuple, coercions );
      }
    catch( Exception exception )
      {
      throw new TupleException( "unable to set into: " + declarator.print() + ", using selector: " + selector.print(), exception );
      }
    }

  /**
   * Method iterator returns an {@link Iterator} over this Tuple instances values.
   *
   * @return Iterator
   */
  public Iterator<Object> iterator()
    {
    return elements.iterator();
    }

  /**
   * Method isEmpty returns true if this Tuple instance has no values.
   *
   * @return the empty (type boolean) of this Tuple object.
   */
  public boolean isEmpty()
    {
    return elements.isEmpty();
    }

  /**
   * Method size returns the number of values in this Tuple instance.
   *
   * @return int
   */
  public int size()
    {
    return elements.size();
    }

  /**
   * Method elements returns a new Object[] array of this Tuple instances values.
   *
   * @return Object[]
   */
  private Object[] elements()
    {
    return elements.toArray();
    }

  /**
   * Method elements returns the given destination array with the values of This tuple instance.
   *
   * @param destination of type Object[]
   * @return Object[]
   */
  <T> T[] elements( T[] destination )
    {
    return elements.toArray( destination );
    }

  /**
   * Method getTypes returns an array of the element classes. Null if the element is null.
   *
   * @return the types (type Class[]) of this Tuple object.
   */
  public Class[] getTypes()
    {
    Class[] types = new Class[ elements.size() ];

    for( int i = 0; i < elements.size(); i++ )
      {
      Object value = elements.get( i );

      if( value != null )
        types[ i ] = value.getClass();
      }

    return types;
    }

  /**
   * Method append appends all the values of the given Tuple instances to a copy of this instance.
   *
   * @param tuples of type Tuple
   * @return Tuple
   */
  public Tuple append( Tuple... tuples )
    {
    Tuple result = new Tuple( this );

    for( Tuple tuple : tuples )
      result.addAll( tuple );

    return result;
    }

  /**
   * Method compareTo compares this Tuple to the given Tuple instance.
   *
   * @param other of type Tuple
   * @return int
   */
  public int compareTo( Tuple other )
    {
    if( other == null || other.elements == null )
      return 1;

    if( other.elements.size() != this.elements.size() )
      return this.elements.size() - other.elements.size();

    for( int i = 0; i < this.elements.size(); i++ )
      {
      Comparable lhs = (Comparable) this.elements.get( i );
      Comparable rhs = (Comparable) other.elements.get( i );

      if( lhs == null && rhs == null )
        continue;

      if( lhs == null )
        return -1;
      else if( rhs == null )
        return 1;

      int c = lhs.compareTo( rhs ); // guaranteed to not be null
      if( c != 0 )
        return c;
      }

    return 0;
    }

  public int compareTo( Comparator[] comparators, Tuple other )
    {
    if( comparators == null )
      return compareTo( other );

    if( other == null || other.elements == null )
      return 1;

    if( other.elements.size() != this.elements.size() )
      return this.elements.size() - other.elements.size();

    if( comparators.length != this.elements.size() )
      throw new IllegalArgumentException( "comparator array not same size as tuple elements" );

    for( int i = 0; i < this.elements.size(); i++ )
      {
      Object lhs = this.elements.get( i );
      Object rhs = other.elements.get( i );

      int c;

      if( comparators[ i ] != null )
        c = comparators[ i ].compare( lhs, rhs );
      else if( lhs == null && rhs == null )
        c = 0;
      else if( lhs == null )
        return -1;
      else if( rhs == null )
        return 1;
      else
        c = ( (Comparable) lhs ).compareTo( rhs ); // guaranteed to not be null

      if( c != 0 )
        return c;
      }

    return 0;
    }

  /**
   * Method compareTo implements the {@link Comparable#compareTo(Object)} method.
   *
   * @param other of type Object
   * @return int
   */
  public int compareTo( Object other )
    {
    if( other instanceof Tuple )
      return compareTo( (Tuple) other );
    else
      return -1;
    }

  @SuppressWarnings({"ForLoopReplaceableByForEach"})
  @Override
  public boolean equals( Object object )
    {
    if( !( object instanceof Tuple ) )
      return false;

    Tuple other = (Tuple) object;

    if( this.elements.size() != other.elements.size() )
      return false;

    for( int i = 0; i < this.elements.size(); i++ )
      {
      Object lhs = this.elements.get( i );
      Object rhs = other.elements.get( i );

      if( lhs == null && rhs == null )
        continue;

      if( lhs == null || rhs == null )
        return false;

      if( !lhs.equals( rhs ) )
        return false;
      }

    return true;
    }

  @Override
  public int hashCode()
    {
    int hash = 1;

    for( Object element : elements )
      hash = 31 * hash + ( element != null ? element.hashCode() : 0 );

    return hash;
    }

  @Override
  public String toString()
    {
    return Util.join( elements, printDelim, true );
    }

  /**
   * Method toString writes this Tuple instance values out to a String delimited by the given String value.
   *
   * @param delim of type String
   * @return String
   */
  public String toString( String delim )
    {
    return Util.join( elements, delim, true );
    }

  /**
   * Method toString writes this Tuple instance values out to a String delimited by the given String value.
   *
   * @param delim     of type String
   * @param printNull of type boolean
   * @return String
   */
  public String toString( String delim, boolean printNull )
    {
    return Util.join( elements, delim, printNull );
    }

  /**
   * Method format uses the {@link Formatter} class for formatting this tuples values into a new string.
   *
   * @param format of type String
   * @return String
   */
  public String format( String format )
    {
    return String.format( format, elements() );
    }

  /**
   * Method print returns a parsable String representation of this Tuple instance.
   *
   * @return String
   */
  public String print()
    {
    return printTo( new StringBuffer() ).toString();
    }

  public StringBuffer printTo( StringBuffer buffer )
    {
    buffer.append( "[" );

    if( elements != null )
      {
      for( int i = 0; i < elements.size(); i++ )
        {
        Object element = elements.get( i );

        if( element instanceof Tuple )
          ( (Tuple) element ).printTo( buffer );
        else if( element == null ) // don't quote nulls to distinguish from null strings
          buffer.append( element );
        else
          buffer.append( "\'" ).append( element ).append( "\'" );

        if( i < elements.size() - 1 )
          buffer.append( ", " );
        }
      }

    buffer.append( "]" );

    return buffer;
    }

  private final void verifyModifiable()
    {
    if( isUnmodifiable )
      throw new UnsupportedOperationException( "this tuple is unmodifiable" );
    }
  }
