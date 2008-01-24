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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

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
 */
public class Tuple implements WritableComparable, Iterable, Serializable
  {
  /** Field elements */
  private List<Comparable> elements = new ArrayList<Comparable>();
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
    Tuple result = new Tuple();

    for( int i = 0; i < size; i++ )
      result.add( null );

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

  /** Constructor Tuple creates a new Tuple instance. */
  public Tuple()
    {
    }

  /**
   * Constructor Tuple creates a new Tuple instance with a single String value.
   *
   * @param value of type String
   */
  public Tuple( String value )
    {
    elements.add( value );
    }

  /**
   * Copy constructor. Does not nest the given Tuple instance within this new instance.
   *
   * @param tuple
   */
  public Tuple( Tuple tuple )
    {
    elements.addAll( tuple.elements );
    }

  public Tuple( Comparable... args )
    {
    Collections.addAll( elements, args );
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

    return value.toString();
    }

  /**
   * Method getFloat returns the element at the given position i as a float.
   *
   * @param i of type int
   * @return float
   */
  public float getFloat( int i )
    {
    Comparable value = get( i );

    if( value instanceof Number )
      return ( (Number) value ).floatValue();
    else
      return Float.parseFloat( value.toString() );
    }

  /**
   * Method getDouble returns the element at the given position i as a double.
   *
   * @param i of type int
   * @return double
   */
  public double getDouble( int i )
    {
    Comparable value = get( i );

    if( value instanceof Number )
      return ( (Number) value ).doubleValue();
    else
      return Double.parseDouble( value.toString() );
    }

  /**
   * Method getInteger returns the element at the given position i as an int.
   *
   * @param i of type int
   * @return int
   */
  public int getInteger( int i )
    {
    Comparable value = get( i );

    if( value instanceof Number )
      return ( (Number) value ).intValue();
    else
      return Integer.parseInt( value.toString() );
    }

  /**
   * Method getLong returns the element at the given position i as an long.
   *
   * @param i of type int
   * @return long
   */
  public long getLong( int i )
    {
    Comparable value = get( i );

    if( value instanceof Number )
      return ( (Number) value ).longValue();
    else
      return Long.parseLong( value.toString() );
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
   * Method append appends all the values of the given Tuple instances to this instance.
   *
   * @param tuples of type Tuple...
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
      else if( element instanceof Tuple )
        {
        WritableUtils.writeVInt( out, 6 );
        ( (Tuple) element ).write( out );
        }
      else if( element instanceof WritableComparable )
        {
        WritableUtils.writeVInt( out, 7 );
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
      {
      int type = WritableUtils.readVInt( in );

      if( type == 0 )
        elements.add( null );
      else if( type == 1 )
        elements.add( WritableUtils.readString( in ) );
      else if( type == 2 )
        elements.add( in.readFloat() );
      else if( type == 3 )
        elements.add( in.readDouble() );
      else if( type == 4 )
        elements.add( WritableUtils.readVInt( in ) );
      else if( type == 5 )
        elements.add( WritableUtils.readVLong( in ) );
      else if( type == 6 )
        elements.add( readNewTuple( in ) );
      else if( type == 7 )
        elements.add( readNewWritable( in ) );
      else
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

  private Tuple readNewTuple( DataInput in ) throws IOException
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
    if( other.elements.size() != this.elements.size() )
      return other.elements.size() < this.elements.size() ? 1 : -1;

    for( int i = 0; i < this.elements.size(); i++ )
      {
      Comparable lhs = this.elements.get( i );
      Comparable rhs = other.elements.get( i );

      if( lhs == null && rhs != null )
        return -1;
      else if( rhs != null && lhs == null )
        return 1;

      if( lhs != null && rhs != null )
        {
        int c = lhs.compareTo( rhs );
        if( c != 0 )
          return c;
        }
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
      if( !this.elements.get( i ).equals( other.elements.get( i ) ) )
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
