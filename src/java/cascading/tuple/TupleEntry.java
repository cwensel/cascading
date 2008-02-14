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

/** Class TupleEntry allows a Tuple instance and its declarating Fields instance to be used as a single object. */
public class TupleEntry
  {
  /** Field fields */
  Fields fields;
  /** Field tuple */
  Tuple tuple;

  /**
   * Method select will select a new Tuple instance from the given set of entries. Entries order is significant to
   * the selector.
   *
   * @param selector of type Fields
   * @param entries  of type TupleEntry
   * @return Tuple
   */
  public static Tuple select( Fields selector, TupleEntry... entries )
    {
    // todo: consider just appending tuples values and just peeking those values
    Tuple result = null;

    // does not do field checks
    if( selector.isAll() )
      {
      for( TupleEntry entry : entries )
        {
        if( result == null )
          result = entry.getTuple();
        else
          result = result.append( entry.getTuple() );
        }

      return result;
      }
    int size = 0;

    for( TupleEntry entry : entries )
      size += entry.size();

    result = Tuple.size( selector.size() );
    int offset = 0;

    for( TupleEntry entry : entries )
      {
      for( int i = 0; i < selector.size(); i++ )
        {
        Comparable field = selector.get( i );

        int pos = 0;
        if( field instanceof String )
          {
          pos = entry.fields.indexOfSafe( field );

          if( pos == -1 )
            continue;
          }
        else
          {
          pos = entry.fields.translatePos( (Integer) field, size ) - offset;

          if( pos >= entry.size() || pos < 0 )
            continue;
          }

        result.set( i, entry.get( pos ) ); // last in wins
        }

      offset += entry.size();
      }

    return result;
    }

  /** Constructor TupleEntry creates a new TupleEntry instance. */
  public TupleEntry()
    {
    this.fields = new Fields();
    }

  /**
   * Constructor TupleEntry creates a new TupleEntry instance.
   *
   * @param fields of type Fields
   */
  public TupleEntry( Fields fields )
    {
    this.fields = fields;
    }

  /**
   * Constructor TupleEntry creates a new TupleEntry instance.
   *
   * @param fields of type Fields
   * @param tuple  of type Tuple
   */
  public TupleEntry( Fields fields, Tuple tuple )
    {
    this.fields = fields;
    this.tuple = tuple;
    }

  /**
   * Constructor TupleEntry creates a new TupleEntry instance.
   *
   * @param tuple of type Tuple
   */
  public TupleEntry( Tuple tuple )
    {
    this.fields = Fields.size( tuple.size() );
    this.tuple = tuple;
    }

  /**
   * Method getFields returns the fields of this TupleEntry object.
   *
   * @return the fields (type Fields) of this TupleEntry object.
   */
  public Fields getFields()
    {
    return fields;
    }

  /**
   * Method getTuple returns the tuple of this TupleEntry object.
   *
   * @return the tuple (type Tuple) of this TupleEntry object.
   */
  public Tuple getTuple()
    {
    return tuple;
    }

  /**
   * Method setTuple sets the tuple of this TupleEntry object.
   *
   * @param tuple the tuple of this TupleEntry object.
   */
  public void setTuple( Tuple tuple )
    {
    this.tuple = tuple;
    }

  /**
   * Method size returns the number of values in this instance.
   *
   * @return int
   */
  public int size()
    {
    return tuple.size();
    }

  /**
   * Method get returns the value in the given position i.
   *
   * @param i of type int
   * @return Comparable
   */
  public Comparable get( int i )
    {
    return tuple.get( i );
    }

  /**
   * Method get returns the value in the given field.
   *
   * @param field of type Comparable
   * @return Comparable
   */
  public Comparable get( Comparable field )
    {
    return tuple.get( fields.getPos( field ) );
    }

  /**
   * Method selectEntry selects the fields specified in selector from this instance.
   *
   * @param selector of type Fields
   * @return TupleEntry
   */
  public TupleEntry selectEntry( Fields selector )
    {
    if( selector == null || selector.isAll() )
      return this;

    try
      {
      return new TupleEntry( Fields.asDeclaration( selector ), tuple.get( this.fields, selector ) );
      }
    catch( Exception exception )
      {
      throw new TupleException( "unable to select from: " + this.fields.print() + ", using selector: " + selector.print(), exception );
      }
    }

  /**
   * Method selectTuple selects the fields specified in selector from this instance.
   *
   * @param selector of type Fields
   * @return Tuple
   */
  public Tuple selectTuple( Fields selector )
    {
    if( selector == null || selector.isAll() )
      return this.tuple;

    try
      {
      return tuple.get( fields, selector );
      }
    catch( Exception exception )
      {
      throw new TupleException( "unable to select from: " + this.fields.print() + ", using selector: " + selector.print(), exception );
      }
    }

  /**
   * Method appendNew appends the given TupleEntry instance to this instance.
   *
   * @param entry of type TupleEntry
   * @return TupleEntry
   */
  public TupleEntry appendNew( TupleEntry entry )
    {
    TupleEntry result = new TupleEntry();

    result.fields = fields.append( entry.fields.isUnknown() ? Fields.size( entry.tuple.size() ) : entry.fields );
    result.tuple = tuple.append( entry.tuple );

    return result;
    }

  @Override
  public String toString()
    {
    if( fields == null )
      return "empty";
    else if( tuple == null )
      return "fields: " + fields.print();
    else
      return "fields: " + fields.print() + " tuple: " + tuple.print();
    }

  }
