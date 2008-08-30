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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

import cascading.operation.Aggregator;
import cascading.pipe.Pipe;
import cascading.util.Util;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * A Tuple represents a set of values. Consider a Tuple the same as a data base record where every value is a column in that table.
 * A Tuple stream would be a set of Tuple instances, which are passed consecutively through a Pipe assembly.
 * <p/>
 * A Tuple is a collection of elements. These elements must be of type Comparable, so that Tuple instances can
 * be compared. Tuple itself is Comparable and subsequently can hold elements of type Tuple.
 * <p/>
 * Tuples are mutable for sake of efficiency. They are also Hadoop Writable so they can be streamed in/out as binary.
 * The obvious limitation here is that what are streamed via Hadoop must also be Hadoop Writable, or simply primitive types.
 * <p/>
 * Since Tuples are mutable, it is not a good idea to hold an instance around with out first copying it via its copy
 * constructor, a subsequent {@link Pipe} could change the Tuple in place. This is especially true for {@link Aggregator}
 * operators.
 */
public class Tuple implements WritableComparable, Iterable, Serializable
  {
  /** Field elements */
  private List<Comparable> elements;
  /** Field printDelim */
  private final String printDelim = "\t";

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
    Tuple result = new Tuple();

    for( int i = 0; i < size; i++ )
      result.add( value );

    return result;
    }

  /**
   * Method parse will parse the {@link #print()} String representation of a Tuple instance and return a new Tuple instance.
   *
   * @param string of type String
   * @return Tuple
   */
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

  protected Tuple( List<Comparable> elements )
    {
    this.elements = elements;
    }

  /** Constructor Tuple creates a new Tuple instance. */
  public Tuple()
    {
    this( new ArrayList<Comparable>() );
    }

  /**
   * Constructor Tuple creates a new Tuple instance with a single String value.
   *
   * @param value of type String
   */
  public Tuple( String value )
    {
    this();
    elements.add( value );
    }

  /**
   * Copy constructor. Does not nest the given Tuple instance within this new instance. Use {@link #add(Comparable)}.
   *
   * @param tuple of type Tuple
   */
  public Tuple( Tuple tuple )
    {
    this();
    elements.addAll( tuple.elements );
    }

  /**
   * Constructor Tuple creates a new Tuple instance with all the given values.
   *
   * @param values of type Comparable...
   */
  public Tuple( Comparable... values )
    {
    this();
    Collections.addAll( elements, values );
    }

  /**
   * Method get returns the element at the given position i.
   *
   * @param i of type int
   * @return Comparable
   */
  public Comparable get( int i )
    {
    return elements.get( i );
    }

  /**
   * Method getString returns the element at the given position i as a String.
   *
   * @param i of type int
   * @return String
   */
  public String getString( int i )
    {
    Comparable value = get( i );

    if( value == null )
      return null;

    return value.toString();
    }

  /**
   * Method getFloat returns the element at the given position i as a float. Zero if null.
   *
   * @param i of type int
   * @return float
   */
  public float getFloat( int i )
    {
    Comparable value = get( i );

    if( value instanceof Number )
      return ( (Number) value ).floatValue();
    else if( value == null )
      return 0;
    else
      return Float.parseFloat( value.toString() );
    }

  /**
   * Method getDouble returns the element at the given position i as a double. Zero if null.
   *
   * @param i of type int
   * @return double
   */
  public double getDouble( int i )
    {
    Comparable value = get( i );

    if( value instanceof Number )
      return ( (Number) value ).doubleValue();
    else if( value == null )
      return 0;
    else
      return Double.parseDouble( value.toString() );
    }

  /**
   * Method getInteger returns the element at the given position i as an int. Zero if null.
   *
   * @param i of type int
   * @return int
   */
  public int getInteger( int i )
    {
    Comparable value = get( i );

    if( value instanceof Number )
      return ( (Number) value ).intValue();
    else if( value == null )
      return 0;
    else
      return Integer.parseInt( value.toString() );
    }

  /**
   * Method getLong returns the element at the given position i as an long. Zero if null.
   *
   * @param i of type int
   * @return long
   */
  public long getLong( int i )
    {
    Comparable value = get( i );

    if( value instanceof Number )
      return ( (Number) value ).longValue();
    else if( value == null )
      return 0;
    else
      return Long.parseLong( value.toString() );
    }

  /**
   * Method getShort returns the element at the given position i as an long. Zero if null.
   *
   * @param i of type int
   * @return long
   */
  public short getShort( int i )
    {
    Comparable value = get( i );

    if( value instanceof Number )
      return ( (Number) value ).shortValue();
    else if( value == null )
      return 0;
    else
      return Short.parseShort( value.toString() );
    }

  /**
   * Method getBoolean returns the element at the given position as a boolean. If the value is (case ignored) the string 'true', a true value
   * will be returned. False if null.
   *
   * @param i of type int
   * @return boolean
   */
  public boolean getBoolean( int i )
    {
    Comparable value = get( i );

    if( value instanceof Boolean )
      return ( (Boolean) value ).booleanValue();
    else if( value == null )
      return false;
    else
      return Boolean.parseBoolean( value.toString() );
    }

  /**
   * Method get will return a new Tuple instace populated with element values from the given array of positions.
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
    if( !declarator.isUnknown() && elements.size() != declarator.size() )
      throw new TupleException( "field declaration: " + declarator.print() + ", does not match tuple: " + print() );

    return get( declarator.getPos( selector ) );
    }

  /**
   * Method is the inverse of {@link #remove(int[])}.
   *
   * @param pos of type int[]
   * @return Tuple
   */
  public Tuple leave( int[] pos )
    {
    Tuple results = remove( pos );

    List temp = results.elements;
    results.elements = this.elements;
    this.elements = temp;

    return results;
    }

  /**
   * Method add adds a new element value to this instance.
   *
   * @param value of type Comparable
   */
  public void add( Comparable value )
    {
    elements.add( value );
    }

  /**
   * Method addAll adds all given values to this instance.
   *
   * @param values of type Comparable...
   */
  public void addAll( Comparable... values )
    {
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
    if( tuple != null )
      elements.addAll( tuple.elements );
    }

  /**
   * Method set sets the given value to the given index position in this instance.
   *
   * @param index of type int
   * @param value of type Comparable
   */
  public void set( int index, Comparable value )
    {
    elements.set( index, value );
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
    int[] pos = declarator.getPos( fields );

    for( int i = 0; i < pos.length; i++ )
      elements.set( pos[ i ], tuple.get( i ) );
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
    Tuple results = new Tuple();

    for( int i : pos )
      results.add( elements.remove( i ) );

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
    return remove( declarator.getPos( selector ) );
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

  /**
   * Method extract creates a new Tuple from the given selector, but sets the values in the current tuple to null.
   * <p/>
   * Use this method in conjunction with {@link #set}.
   *
   * @param declarator of type Fields
   * @param selector   of type Fields
   * @return Tuple
   */
  public Tuple extract( Fields declarator, Fields selector )
    {
    return extract( declarator.getPos( selector ) );
    }

  /**
   * Sets the values in the given positions to the values from the given Tuple.
   *
   * @param pos   of type int[]
   * @param tuple of type Tuple
   */
  void set( int[] pos, Tuple tuple )
    {
    if( pos.length != tuple.size() )
      throw new TupleException( "given tuple not same size as position array, tuple: " + tuple.print() );

    int count = 0;
    for( int i : pos )
      elements.set( i, tuple.elements.get( count++ ) );
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
    set( declarator.getPos( selector ), tuple );
    }

  /**
   * Method iterator returns an {@link Iterator} over this Tuple instances values.
   *
   * @return Iterator
   */
  public Iterator iterator()
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
   * Method size retuns the number of values in this Tuple instance.
   *
   * @return int
   */
  public int size()
    {
    return elements.size();
    }

  /**
   * Method elements returns a new Comparable[] array of this Tuple instances values.
   *
   * @return Object[]
   */
  private Object[] elements()
    {
    return elements.toArray();
    }

  /**
   * Method getTypes returns an array of the element classes. Null if the element is null.
   *
   * @return the types (type Class[]) of this Tuple object.
   */
  public Class[] getTypes()
    {
    Class[] types = new Class[elements.size()];

    for( int i = 0; i < elements.size(); i++ )
      {
      Comparable value = elements.get( i );

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
   * Method write is used by Hadoop to write this Tuple instance out to a file.
   *
   * @param out of type DataOutput
   * @throws IOException when
   */
  public void write( DataOutput out ) throws IOException
    {
    WritableUtils.writeVInt( out, elements.size() );

    for( Object element : elements )
      {
      if( element == null )
        {
        WritableUtils.writeVInt( out, 0 );
        }
      else if( element instanceof String )
        {
        WritableUtils.writeVInt( out, 1 );
        WritableUtils.writeString( out, (String) element );
        }
      else if( element instanceof Float )
        {
        WritableUtils.writeVInt( out, 2 );
        out.writeFloat( (Float) element );
        }
      else if( element instanceof Double )
        {
        WritableUtils.writeVInt( out, 3 );
        out.writeDouble( (Double) element );
        }
      else if( element instanceof Integer )
        {
        WritableUtils.writeVInt( out, 4 );
        WritableUtils.writeVInt( out, (Integer) element );
        }
      else if( element instanceof Long )
        {
        WritableUtils.writeVInt( out, 5 );
        WritableUtils.writeVLong( out, (Long) element );
        }
      else if( element instanceof Short )
        {
        WritableUtils.writeVInt( out, 6 );
        out.writeShort( (Short) element );
        }
      else if( element instanceof Tuple )
        {
        WritableUtils.writeVInt( out, 7 );
        ( (Tuple) element ).write( out );
        }
      else if( element instanceof WritableComparable )
        {
        WritableUtils.writeVInt( out, 8 );
        WritableUtils.writeString( out, element.getClass().getName() );
        ( (WritableComparable) element ).write( out );
        }
      else
        {
        throw new IOException( "could not write unknown element type: " + element.getClass().getName() );
        }
      }
    }

  /**
   * Method readFields is used by Hadoop to read this Tuple instance from a file.
   *
   * @param in of type DataInput
   * @throws IOException when
   */
  public void readFields( DataInput in ) throws IOException
    {
    elements.clear();
    int len = WritableUtils.readVInt( in );

    for( int i = 0; i < len; i++ )
      elements.add( readType( WritableUtils.readVInt( in ), in ) );
    }

  private final Comparable readType( int type, DataInput in ) throws IOException
    {
    switch( type )
      {
      case 0:
        return null;
      case 1:
        return WritableUtils.readString( in );
      case 2:
        return in.readFloat();
      case 3:
        return in.readDouble();
      case 4:
        return WritableUtils.readVInt( in );
      case 5:
        return WritableUtils.readVLong( in );
      case 6:
        return in.readShort();
      case 7:
        return readNewTuple( in );
      case 8:
        return readNewWritable( in );
      default:
        throw new IOException( "could not read unknown element type: " + type );
      }
    }

  private Comparable readNewWritable( DataInput in ) throws IOException
    {
    String className = WritableUtils.readString( in );

    try
      {
      Class type = Class.forName( className );
      WritableComparable result = (WritableComparable) type.newInstance();

      result.readFields( in );

      return result;
      }
    catch( ClassNotFoundException exception )
      {
      throw new TupleException( "unable to load WritableComparable named: " + className, exception );
      }
    catch( IllegalAccessException exception )
      {
      throw new TupleException( "unable to access WritableComparable named: " + className, exception );
      }
    catch( InstantiationException exception )
      {
      throw new TupleException( "unable to instantiate WritableComparable named: " + className, exception );
      }
    }

  /**
   * Read a new Tuple instance from the given DataInput stream.
   *
   * @param in of type DataInput
   * @return Tuple
   * @throws IOException
   */
  public static Tuple readNewTuple( DataInput in ) throws IOException
    {
    Tuple tuple = new Tuple();

    tuple.readFields( in );

    return tuple;
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
      Comparable lhs = this.elements.get( i );
      Comparable rhs = other.elements.get( i );

      if( lhs == null && rhs == null )
        continue;

      if( lhs == null && rhs != null )
        return -1;
      else if( lhs != null && rhs == null )
        return 1;

      int c = lhs.compareTo( rhs );
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
      Comparable lhs = this.elements.get( i );
      Comparable rhs = other.elements.get( i );

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

    for( Comparable element : elements )
      hash = 31 * hash + ( element != null ? element.hashCode() : 0 );

    return hash;
    }

  @Override
  public String toString()
    {
    return Util.join( elements, printDelim );
    }

  /**
   * Method toString writes this Tuple instance values out to a String delimited by the given String value.
   *
   * @param delim of type String
   * @return String
   */
  public String toString( String delim )
    {
    return Util.join( elements, delim );
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
    return print( new StringBuffer() ).toString();
    }

  private StringBuffer print( StringBuffer buffer )
    {
    buffer.append( "[" );
    for( int i = 0; i < elements.size(); i++ )
      {
      Comparable element = elements.get( i );

      if( element instanceof Tuple )
        ( (Tuple) element ).print( buffer );
      else
        buffer.append( "\'" ).append( element ).append( "\'" );

      if( i < elements.size() - 1 )
        buffer.append( ", " );
      }
    buffer.append( "]" );

    return buffer;
    }

  }
