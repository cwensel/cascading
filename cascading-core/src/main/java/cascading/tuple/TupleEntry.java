/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
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
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Iterator;

import cascading.tuple.coerce.Coercions;
import cascading.tuple.type.CoercibleType;
import cascading.util.ForeverValueIterator;

/**
 * Class TupleEntry allows a {@link Tuple} instance and its declaring {@link Fields} instance to be used as a single object.
 * <p/>
 * Once a TupleEntry is created, its Fields cannot be changed, but the Tuple instance it holds can be replaced or
 * modified. The managed Tuple should not have elements added or removed, as this will break the relationship with
 * the associated Fields instance.
 * <p/>
 * If type information is provided on the Fields instance, all setters on this class will use that information to
 * coerce the given object to the expected type.
 * <p/>
 * For example, if position is is of type {@code long}, then {@code entry.setString(0, "9" )} will coerce the "9" to a
 * long {@code 9}. Thus, {@code entry.getObject(0) == 9l}.
 * <p/>
 * No coercion is performed with the {@link #getObject(Comparable)} and {@link #getObject(int)} methods.
 * <p/>
 * To set a value without coercion, see the {@link #setRaw(Comparable, Object)} and {@link #setRaw(int, Object)}
 * methods.
 *
 * @see Fields
 * @see Tuple
 */
public class TupleEntry
  {
  private static final CoercibleType[] EMPTY_COERCIONS = new CoercibleType[ 0 ];
  private static final ForeverValueIterator<CoercibleType> OBJECT_ITERATOR = new ForeverValueIterator<CoercibleType>( Coercions.OBJECT );

  /** An EMPTY TupleEntry instance for use as a stand in instead of a {@code null}. */
  public static final TupleEntry NULL = new TupleEntry( Fields.NONE, Tuple.NULL );

  /** Field fields */
  private Fields fields;

  private CoercibleType[] coercions = EMPTY_COERCIONS;

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

        int pos;

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
    this.fields = Fields.NONE;

    setCoercions();
    }

  /**
   * Constructor TupleEntry creates a new TupleEntry instance.
   *
   * @param isUnmodifiable of type boolean
   */
  @ConstructorProperties({"isUnmodifiable"})
  public TupleEntry( boolean isUnmodifiable )
    {
    this.fields = Fields.NONE;
    this.isUnmodifiable = isUnmodifiable;

    setCoercions();
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

    setCoercions();
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

    setCoercions();
    }

  /**
   * Constructor TupleEntry creates a new TupleEntry instance.
   *
   * @param fields         of type Fields
   * @param tuple          of type Tuple
   * @param isUnmodifiable of type boolean
   */
  @ConstructorProperties({"fields", "tuple", "isUnmodifiable"})
  public TupleEntry( Fields fields, Tuple tuple, boolean isUnmodifiable )
    {
    this.fields = fields;
    this.isUnmodifiable = isUnmodifiable;
    setTuple( tuple );

    setCoercions();
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

    setCoercions();
    }

  /**
   * Constructor TupleEntry creates a new TupleEntry instance that is a safe copy of the given tupleEntry.
   * <p/>
   * The new instance is safe to cache and will be modifiable regardless of the given tupleEntry state.
   *
   * @param tupleEntry of type TupleEntry
   */
  @ConstructorProperties({"tupleEntry"})
  public TupleEntry( TupleEntry tupleEntry )
    {
    this.fields = tupleEntry.getFields();
    this.tuple = tupleEntry.getTupleCopy();

    setCoercions();
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

    setCoercions();
    }

  private void setCoercions()
    {
    if( coercions != EMPTY_COERCIONS )
      return;

    coercions = getCoercions( getFields(), tuple );
    }

  static CoercibleType[] getCoercions( Fields fields, Tuple tuple )
    {
    Type[] types = fields.types; // safe to not get a copy
    int size = fields.size();

    size = size == 0 && tuple != null ? tuple.size() : size;

    if( size == 0 )
      return EMPTY_COERCIONS;

    return Coercions.coercibleArray( size, types );
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
   * Returns true if there are types associated with this instance.
   *
   * @return boolean
   */
  public boolean hasTypes()
    {
    return fields.hasTypes();
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
   * Method getCoercedTuple is a helper method for copying the current tuple elements into a new Tuple,
   * of the same size, as the requested coerced types.
   *
   * @param types of type Type[]
   * @return returns the a new Tuple instance with coerced values
   */
  public Tuple getCoercedTuple( Type[] types )
    {
    return getCoercedTuple( types, Tuple.size( types.length ) );
    }

  /**
   * Method getCoercedTuple is a helper method for copying the current tuple elements into the new Tuple,
   * of the same size, as the requested coerced types.
   *
   * @param types of type Type[]
   * @param into  of type Tuple
   * @return returns the given into Tuple instance with coerced values
   */
  public Tuple getCoercedTuple( Type[] types, Tuple into )
    {
    if( into == null )
      throw new IllegalArgumentException( "into argument Tuple may not be null" );

    if( coercions.length != types.length || types.length != into.size() )
      throw new IllegalArgumentException( "current entry and given tuple and types must be same length" );

    for( int i = 0; i < coercions.length; i++ )
      {
      Object element = tuple.getObject( i );
      into.set( i, Coercions.coerce( coercions[ i ], element, types[ i ] ) );
      }

    return into;
    }

  /**
   * Method setTuple sets the tuple of this TupleEntry object, no copy will be performed.
   * <p/>
   * If the given tuple is "unmodifiable" ({@code Tuple.isUnmodifiable() == true}) and this TupleEntry is
   * not "unmodifiable", an exception will be thrown.
   * <p/>
   * Unmodifiable tuples are generally owned by the system and cannot be be changed and must not be cached.
   *
   * @param tuple the tuple of this TupleEntry object.
   */
  public void setTuple( Tuple tuple )
    {
    if( !isUnmodifiable && tuple != null && tuple.isUnmodifiable() )
      throw new IllegalArgumentException( "current entry is modifiable but given tuple is not modifiable, make copy of given Tuple first" );

    if( tuple != null && isUnmodifiable )
      this.tuple = Tuples.asUnmodifiable( tuple );
    else
      this.tuple = tuple;

    setCoercions();
    }

  /**
   * Method setCanonicalTuple replaces each value of the current tuple with the given tuple elements after
   * they are coerced.
   * <p/>
   * This method will modify the existing Tuple wrapped by this TupleEntry instance even
   * if it is marked as unmodifiable.
   * <p/>
   * If tuple argument is {@code null}, the current tuple will be set to {@code null}.
   *
   * @param tuple to replace the current wrapped Tuple instance
   */
  public void setCanonicalTuple( Tuple tuple )
    {
    if( tuple == null )
      {
      this.tuple = null;
      return;
      }

    if( isUnmodifiable )
      tuple = Tuples.asUnmodifiable( tuple );

    if( fields.size() != tuple.size() )
      throw new IllegalArgumentException( "current entry and given tuple must be same length" );

    for( int i = 0; i < coercions.length; i++ )
      {
      Object element = tuple.getObject( i );

      this.tuple.set( i, coercions[ i ].canonical( element ) ); // force read type to the expected type
      }
    }

  /**
   * Method setCanonicalValues replaces each value of the current tuple with th give Object[]
   * after they are coerced.
   *
   * @param values to replace the current wrapped tuple instance values
   */
  public void setCanonicalValues( Object[] values )
    {
    if( fields.size() != values.length )
      throw new IllegalArgumentException( "current entry and given array must be same length" );

    for( int i = 0; i < coercions.length; i++ )
      {
      Object element = values[ i ];

      this.tuple.set( i, coercions[ i ].canonical( element ) ); // force read type to the expected type
      }
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
   * Method getObject returns the value in the given position pos.
   * <p/>
   * No coercion is performed if there is an associated coercible type.
   *
   * @param pos position of the element to return.
   * @return Object
   */
  public Object getObject( int pos )
    {
    return tuple.getObject( pos );
    }

  /**
   * Method getObject returns the value in the given field or position as the requested type.
   * <p/>
   * Coercion is performed to the given type.
   *
   * @param pos position of the element to return.
   * @return Object
   */
  public Object getObject( int pos, Type type )
    {
    return Coercions.coerce( coercions[ pos ], tuple.getObject( pos ), type );
    }

  /**
   * Method getObject returns the value in the given field or position.
   * <br/>
   * {@code fieldName} may optionally be a {@link Fields} instance. Only the first field name or position will
   * be considered.
   * <p/>
   * No coercion is performed if there is an associated coercible type.
   *
   * @param fieldName field name or position to return
   * @return Comparable
   */
  public Object getObject( Comparable fieldName )
    {
    int pos = fields.getPos( asFieldName( fieldName ) );
    return tuple.getObject( pos );
    }

  /**
   * Method getObject returns the value in the given field or position as the requested type.
   * <br/>
   * {@code fieldName} may optionally be a {@link Fields} instance. Only the first field name or position will
   * be considered.
   * <p/>
   * Coercion is performed to the given type.
   *
   * @param fieldName field name or position to return
   * @return Comparable
   */
  public Object getObject( Comparable fieldName, Type type )
    {
    int pos = fields.getPos( asFieldName( fieldName ) );
    return Coercions.coerce( coercions[ pos ], tuple.getObject( pos ), type );
    }

  /**
   * Method set sets the value in the given position.
   * <p/>
   * No coercion is performed if there is an associated coercible type.
   *
   * @param pos   position to set
   * @param value of type Comparable
   */
  public void setRaw( int pos, Object value )
    {
    tuple.set( pos, value );
    }

  /**
   * Method set sets the value in the given field or position.
   * <p/>
   * No coercion is performed if there is an associated coercible type.
   *
   * @param fieldName field name or position to set
   * @param value     of type Comparable
   */
  public void setRaw( Comparable fieldName, Object value )
    {
    tuple.set( fields.getPos( asFieldName( fieldName ) ), value );
    }

  /**
   * Method set sets the value in the given field or position.
   *
   * @param fieldName field name or position to set
   * @param value     of type Comparable
   */
  public void setObject( Comparable fieldName, Object value )
    {
    int pos = fields.getPos( asFieldName( fieldName ) );

    tuple.set( pos, coercions[ pos ].canonical( value ) );
    }

  /**
   * Method setBoolean sets the value in the given field or position.
   *
   * @param fieldName field name or position to set
   * @param value     of type boolean
   */
  public void setBoolean( Comparable fieldName, boolean value )
    {
    int pos = fields.getPos( asFieldName( fieldName ) );

    tuple.set( pos, coercions[ pos ].canonical( value ) );
    }

  /**
   * Method setShort sets the value in the given field or position.
   *
   * @param fieldName field name or position to set
   * @param value     of type short
   */
  public void setShort( Comparable fieldName, short value )
    {
    int pos = fields.getPos( asFieldName( fieldName ) );

    tuple.set( pos, coercions[ pos ].canonical( value ) );
    }

  /**
   * Method setInteger sets the value in the given field or position.
   *
   * @param fieldName field name or position to set
   * @param value     of type int
   */
  public void setInteger( Comparable fieldName, int value )
    {
    int pos = fields.getPos( asFieldName( fieldName ) );

    tuple.set( pos, coercions[ pos ].canonical( value ) );
    }

  /**
   * Method setLong sets the value in the given field or position.
   *
   * @param fieldName field name or position to set
   * @param value     of type long
   */
  public void setLong( Comparable fieldName, long value )
    {
    int pos = fields.getPos( asFieldName( fieldName ) );

    tuple.set( pos, coercions[ pos ].canonical( value ) );
    }

  /**
   * Method setFloat sets the value in the given field or position.
   *
   * @param fieldName field name or position to set
   * @param value     of type float
   */
  public void setFloat( Comparable fieldName, float value )
    {
    int pos = fields.getPos( asFieldName( fieldName ) );

    tuple.set( pos, coercions[ pos ].canonical( value ) );
    }

  /**
   * Method setDouble sets the value in the given field or position.
   *
   * @param fieldName field name or position to set
   * @param value     of type double
   */
  public void setDouble( Comparable fieldName, double value )
    {
    int pos = fields.getPos( asFieldName( fieldName ) );

    tuple.set( pos, coercions[ pos ].canonical( value ) );
    }

  /**
   * Method setString sets the value in the given field or position.
   *
   * @param fieldName field name or position to set
   * @param value     of type String
   */
  public void setString( Comparable fieldName, String value )
    {
    int pos = fields.getPos( asFieldName( fieldName ) );
    tuple.set( pos, coercions[ pos ].canonical( value ) );
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
    return (String) getObject( fieldName, String.class );
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
    return (Float) getObject( fieldName, float.class );
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
    return (Double) getObject( fieldName, double.class );
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
    return (Integer) getObject( fieldName, int.class );
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
    return (Long) getObject( fieldName, long.class );
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
    return (Short) getObject( fieldName, short.class );
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
    return (Boolean) getObject( fieldName, boolean.class );
    }

  private Comparable asFieldName( Comparable fieldName )
    {
    return Fields.asFieldName( fieldName );
    }

  /**
   * Method selectEntry selects the fields specified in the selector from this instance. If {@link Fields#ALL} or the
   * same fields as declared are given, {@code this} will be returned.
   * <p/>
   * The returned TupleEntry will be either modifiable or unmodifiable, depending on the state of this TupleEntry instance.
   * <p/>
   * See {@link #selectEntryCopy(Fields)} to guarantee a copy suitable for modifying or caching/storing in a collection.
   * <p/>
   * Note this is a bug fix and change from 2.0 and 2.1. In previous versions the modifiable state was dependent
   * on the given selector.
   *
   * @param selector Fields selector that selects the values to return
   * @return TupleEntry
   */
  public TupleEntry selectEntry( Fields selector )
    {
    if( selector == null || selector.isAll() || fields == selector ) // == is intentional
      return this;

    if( selector.isNone() )
      return isUnmodifiable ? TupleEntry.NULL : new TupleEntry();

    return new TupleEntry( Fields.asDeclaration( selector ), tuple.get( this.fields, selector ), isUnmodifiable );
    }

  /**
   * Method selectEntry selects the fields specified in selector from this instance.
   * <p/>
   * It is guaranteed to return a new modifiable TupleEntry instance at a cost of copying data.
   * <p/>
   * The returned instance is safe to cache.
   *
   * @param selector Fields selector that selects the values to return
   * @return TupleEntry
   */
  public TupleEntry selectEntryCopy( Fields selector )
    {
    if( selector == null || selector.isAll() || fields == selector ) // == is intentional
      return new TupleEntry( this );

    if( selector.isNone() )
      return new TupleEntry();

    return new TupleEntry( Fields.asDeclaration( selector ), tuple.get( this.fields, selector ) );
    }

  /**
   * Method selectTuple selects the fields specified in the selector from this instance. If {@link Fields#ALL} or the
   * same fields as declared are given, {@code this.getTuple()} will be returned.
   * <p/>
   * The returned Tuple will be either modifiable or unmodifiable, depending on the state of this TupleEntry instance.
   * <p/>
   * See {@link #selectTupleCopy(Fields)} to guarantee a copy suitable for modifying or caching/storing in a collection.
   * <p/>
   * Note this is a bug fix and change from 2.0 and 2.1. In previous versions the modifiable state was dependent
   * on the given selector.
   *
   * @param selector Fields selector that selects the values to return
   * @return Tuple
   */
  public Tuple selectTuple( Fields selector )
    {
    if( selector == null || selector.isAll() || fields == selector ) // == is intentional
      return this.tuple;

    if( selector.isNone() )
      return Tuple.NULL;

    Tuple result = tuple.get( fields, selector );

    if( isUnmodifiable )
      Tuples.asUnmodifiable( result );

    return result;
    }

  /**
   * Method selectTupleCopy selects the fields specified in selector from this instance.
   * <p/>
   * It is guaranteed to return a new modifiable Tuple instance at a cost of copying data.
   * <p/>
   * The returned instance is safe to cache.
   *
   * @param selector Fields selector that selects the values to return
   * @return Tuple
   */
  public Tuple selectTupleCopy( Fields selector )
    {
    if( selector == null || selector.isAll() || fields == selector ) // == is intentional
      return new Tuple( this.tuple );

    if( selector.isNone() )
      return new Tuple();

    return tuple.get( fields, selector );
    }

  /**
   * Method selectInto selects the fields specified in the selector from this instance and copies
   * them into the given tuple argument.
   *
   * @param selector of type Fields
   * @param tuple    of type Tuple
   * @return returns the given tuple argument with new values added
   */
  public Tuple selectInto( Fields selector, Tuple tuple )
    {
    if( selector.isNone() )
      return tuple;

    int[] pos = this.tuple.getPos( fields, selector );

    if( pos == null || pos.length == 0 )
      {
      tuple.addAll( this.tuple );
      }
    else
      {
      for( int i : pos )
        tuple.add( this.tuple.getObject( i ) );
      }

    return tuple;
    }

  /**
   * Method setTuple sets the values specified by the selector to the values given by the given tuple, the given
   * values will always be copied into this TupleEntry.
   *
   * @param selector of type Fields
   * @param tuple    of type Tuple
   */
  public void setTuple( Fields selector, Tuple tuple )
    {
    if( selector == null || selector.isAll() )
      this.tuple.setAll( tuple );
    else
      this.tuple.set( fields, selector, tuple );
    }

  /**
   * Method set sets the values from the given tupleEntry into this TupleEntry instance based on the given
   * tupleEntry field names.
   * <p/>
   * If type information is given, each incoming value will be coerced from its canonical type to the current type.
   *
   * @param tupleEntry of type TupleEntry
   */
  public void set( TupleEntry tupleEntry )
    {
    this.tuple.set( fields, tupleEntry.getFields(), tupleEntry.getTuple(), tupleEntry.coercions );
    }

  /**
   * Method appendNew appends the given TupleEntry instance to this instance.
   *
   * @param entry of type TupleEntry
   * @return TupleEntry
   */
  public TupleEntry appendNew( TupleEntry entry )
    {
    Fields appendedFields = fields.append( entry.fields.isUnknown() ? Fields.size( entry.tuple.size() ) : entry.fields );
    Tuple appendedTuple = tuple.append( entry.tuple );

    return new TupleEntry( appendedFields, appendedTuple );
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;

    if( !( object instanceof TupleEntry ) )
      return false;

    TupleEntry that = (TupleEntry) object;

    if( fields != null ? !fields.equals( that.fields ) : that.fields != null )
      return false;

    // use comparators if in the this side fields instance
    if( tuple != null ? fields.compare( tuple, that.tuple ) != 0 : that.tuple != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = fields != null ? fields.hashCode() : 0;
    result = 31 * result + ( tuple != null ? tuple.hashCode() : 0 );
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

  /**
   * Method asIterableOf returns an {@link Iterable} instance that will coerce all Tuple elements
   * into the given {@code type} parameter.
   * <p/>
   * This method honors any {@link cascading.tuple.type.CoercibleType} instances on the internal
   * Fields instance for the specified Tuple element.
   *
   * @param type of type Class
   * @return an Iterable
   */
  public <T> Iterable<T> asIterableOf( final Class<T> type )
    {
    return new Iterable<T>()
    {
    @Override
    public Iterator<T> iterator()
      {
      final Iterator<CoercibleType> coercibleIterator = coercions.length == 0 ?
        OBJECT_ITERATOR :
        Arrays.asList( coercions ).iterator();

      final Iterator valuesIterator = tuple.iterator();

      return new Iterator<T>()
      {
      @Override
      public boolean hasNext()
        {
        return valuesIterator.hasNext();
        }

      @Override
      public T next()
        {
        Object next = valuesIterator.next();

        return (T) coercibleIterator.next().coerce( next, type );
        }

      @Override
      public void remove()
        {
        valuesIterator.remove();
        }
      };
      }
    };
    }
  }
