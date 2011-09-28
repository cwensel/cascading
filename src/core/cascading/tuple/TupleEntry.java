/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

/**
 * Class TupleEntry allows a {@link Tuple} instance and its declaring {@link Fields} instance to be used as a single object.
 * <p/>
 * Once a TupleEntry is created, its Fields cannot be changed, but the Tuple instance it holds can be replaced or
 * modified. The managed Tuple should not have elements added or removed, as this will break the relationship with
 * the associated Fields instance.
 *
 * @see Fields
 * @see Tuple
 */
public class TupleEntry
  {
  /** Field fields */
  Fields fields;
  /** Field isUnmodifiable */
  private boolean isUnmodifiable = false;
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

        result.set( i, entry.getObject( pos ) ); // last in wins
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
   * @param isUnmodifiable of type boolean
   */
  @ConstructorProperties({"isUnmodifiable"})
  public TupleEntry( boolean isUnmodifiable )
    {
    this.fields = new Fields();
    this.isUnmodifiable = isUnmodifiable;
    }

  /**
   * Constructor TupleEntry creates a new TupleEntry instance.
   *
   * @param fields of type Fields
   */
  @ConstructorProperties({"fields"})
  public TupleEntry( Fields fields )
    {
    this.fields = fields;
    }

  /**
   * Constructor TupleEntry creates a new TupleEntry instance.
   *
   * @param fields         of type Fields
   * @param isUnmodifiable of type boolean
   */
  @ConstructorProperties({"fields", "isUnmodifiable"})
  public TupleEntry( Fields fields, boolean isUnmodifiable )
    {
    this.fields = fields;
    this.isUnmodifiable = isUnmodifiable;
    }

  /**
   * Constructor TupleEntry creates a new TupleEntry instance.
   *
   * @param fields of type Fields
   * @param tuple  of type Tuple
   */
  @ConstructorProperties({"fields", "tuple"})
  public TupleEntry( Fields fields, Tuple tuple )
    {
    this.fields = fields;
    this.tuple = tuple;
    }

  /**
   * Constructor TupleEntry creates a new TupleEntry instance that is a safe copy of the given tupleEntry.
   *
   * @param tupleEntry of type TupleEntry
   */
  @ConstructorProperties({"tupleEntry"})
  public TupleEntry( TupleEntry tupleEntry )
    {
    this.fields = tupleEntry.fields;
    this.tuple = new Tuple( tupleEntry.getTuple() );
    }

  /**
   * Constructor TupleEntry creates a new TupleEntry instance.
   *
   * @param tuple of type Tuple
   */
  @ConstructorProperties({"tuple"})
  public TupleEntry( Tuple tuple )
    {
    this.fields = Fields.size( tuple.size() );
    this.tuple = tuple;
    }

  /**
   * Method isUnmodifiable returns true if this TupleEntry is unmodifiable.
   *
   * @return boolean
   */
  public boolean isUnmodifiable()
    {
    return isUnmodifiable;
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
   * Method getTupleCopy returns a copy of the tuple of this TupleEntry object.
   *
   * @return a copy of the tuple (type Tuple) of this TupleEntry object.
   */
  public Tuple getTupleCopy()
    {
    return new Tuple( tuple );
    }

  /**
   * Method setTuple sets the tuple of this TupleEntry object.
   *
   * @param tuple the tuple of this TupleEntry object.
   */
  public void setTuple( Tuple tuple )
    {
    if( isUnmodifiable )
      this.tuple = Tuples.asUnmodifiable( tuple );
    else
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
   * Method get returns the value in the given position pos.
   *
   * @param pos position of the element to return.
   * @return Comparable
   */
  public Comparable get( int pos )
    {
    return tuple.get( pos );
    }

  /**
   * Method get returns the value in the given field or position.
   * <br/>
   * {@code fieldName} may optionally be a {@link Fields} instance. Only the first field name or position will
   * be considered.
   *
   * @param fieldName field name or position to return
   * @return Comparable
   */
  public Comparable get( Comparable fieldName )
    {
    return tuple.get( fields.getPos( asFieldName( fieldName ) ) );
    }

  /**
   * Method get returns the value in the given field or position.
   * <br/>
   * {@code fieldName} may optionally be a {@link Fields} instance. Only the first field name or position will
   * be considered.
   *
   * @param fieldName field name or position to return
   * @return Comparable
   */
  public Object getObject( Comparable fieldName )
    {
    return tuple.getObject( fields.getPos( asFieldName( fieldName ) ) );
    }

  /**
   * Method set sets the value in the given field or position.
   *
   * @param fieldName field name or position to set
   * @param value     of type Comparable
   */
  public void set( Comparable fieldName, Object value )
    {
    tuple.set( fields.getPos( asFieldName( fieldName ) ), value );
    }

  /**
   * Method getString returns the element for the given field name or position as a String.
   * <br/>
   * {@code fieldName} may optionally be a {@link Fields} instance. Only the first field name or position will
   * be considered.
   *
   * @param fieldName field name or position to return
   * @return String
   */
  public String getString( Comparable fieldName )
    {
    return tuple.getString( fields.getPos( asFieldName( fieldName ) ) );
    }

  /**
   * Method getFloat returns the element for the given field name or position as a float. Zero if null.
   * <br/>
   * {@code fieldName} may optionally be a {@link Fields} instance. Only the first field name or position will
   * be considered.
   *
   * @param fieldName field name or position to return
   * @return float
   */
  public float getFloat( Comparable fieldName )
    {
    return tuple.getFloat( fields.getPos( asFieldName( fieldName ) ) );
    }

  /**
   * Method getDouble returns the element for the given field name or position as a double. Zero if null.
   * <br/>
   * {@code fieldName} may optionally be a {@link Fields} instance. Only the first field name or position will
   * be considered.
   *
   * @param fieldName field name or position to return
   * @return double
   */
  public double getDouble( Comparable fieldName )
    {
    return tuple.getDouble( fields.getPos( asFieldName( fieldName ) ) );
    }

  /**
   * Method getInteger  returns the element for the given field name or position as an int. Zero if null.
   * <br/>
   * {@code fieldName} may optionally be a {@link Fields} instance. Only the first field name or position will
   * be considered.
   *
   * @param fieldName field name or position to return
   * @return int
   */
  public int getInteger( Comparable fieldName )
    {
    return tuple.getInteger( fields.getPos( asFieldName( fieldName ) ) );
    }

  /**
   * Method getLong returns the element for the given field name or position as a long. Zero if null.
   * <br/>
   * {@code fieldName} may optionally be a {@link Fields} instance. Only the first field name or position will
   * be considered.
   *
   * @param fieldName field name or position to return
   * @return long
   */
  public long getLong( Comparable fieldName )
    {
    return tuple.getLong( fields.getPos( asFieldName( fieldName ) ) );
    }

  /**
   * Method getShort returns the element for the given field name or position as a short. Zero if null.
   * <br/>
   * {@code fieldName} may optionally be a {@link Fields} instance. Only the first field name or position will
   * be considered.
   *
   * @param fieldName field name or position to return
   * @return short
   */
  public short getShort( Comparable fieldName )
    {
    return tuple.getShort( fields.getPos( asFieldName( fieldName ) ) );
    }

  /**
   * Method getBoolean returns the element for the given field name or position as a boolean.
   * If the value is (case ignored) the string 'true', a {@code true} value will be returned. {@code false} if null.
   * <br/>
   * {@code fieldName} may optionally be a {@link Fields} instance. Only the first field name or position will
   * be considered.
   *
   * @param fieldName field name or position to return
   * @return boolean
   */
  public boolean getBoolean( Comparable fieldName )
    {
    return tuple.getBoolean( fields.getPos( asFieldName( fieldName ) ) );
    }

  private Comparable asFieldName( Comparable fieldName )
    {
    return Fields.asFieldName( fieldName );
    }

  /**
   * Method selectEntry selects the fields specified in selector from this instance.
   *
   * @param selector Fields selector that selects the values to return
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
   * @param selector Fields selector that selects the values to return
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
   * Method selectInteger selects the first field Tuple value in the specified selector.
   * <br/>
   * All other fields in the selector are ignored.
   *
   * @param selector
   * @return an int value
   */
  public int selectInteger( Fields selector )
    {
    if( selector.isDefined() )
      throw new TupleException( "given selector must define a field name or position to select with" );

    return tuple.getInteger( fields.getPos( selector.get( 0 ) ) );
    }

  /**
   * Method setTuple sets the values specified by the selector to the values given by the given tuple.
   *
   * @param selector of type Fields
   * @param tuple    of type Tuple
   */
  public void setTuple( Fields selector, Tuple tuple )
    {
    if( selector == null || selector.isAll() )
      {
      this.tuple = tuple;
      return;
      }

    try
      {
      this.tuple.set( fields, selector, tuple );
      }
    catch( Exception exception )
      {
      throw new TupleException( "unable to select from: " + this.fields.print() + ", using selector: " + selector.print(), exception );
      }
    }

  /**
   * Method set sets the values from the given tupleEntry into this TupleEntry instance based on the given
   * tupleEntry field names.
   *
   * @param tupleEntry of type TupleEntry
   */
  public void set( TupleEntry tupleEntry )
    {
    try
      {
      this.tuple.set( fields, tupleEntry.getFields(), tupleEntry.getTuple() );
      }
    catch( Exception exception )
      {
      throw new TupleException( "unable to select from: " + this.fields.print() + ", using selector: " + tupleEntry.getFields().print(), exception );
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
