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
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.tap.Tap;
import cascading.util.Util;

/**
 * Class Fields represents the field names in a {@link Tuple}. A tuple field may be a literal String value representing a
 * name, or it may be a literal Integer value representing a position, where positions start at position 0.
 * A Fields instance may also represent a set of field names and positions.
 * <p/>
 * Fields are used as both declarators and selectors. A declarator declares that a given {@link Tap} or
 * {@link cascading.operation.Operation} returns the given field names, for a set of values the size of
 * the given Fields instance. A selector is used to select given referenced fields from a Tuple.
 * For example; <br/>
 * <code>Fields fields = new Fields( "a", "b", "c" );</code><br/>
 * This creates a new Fields instance with the field names "a", "b", and "c". This Fields instance can be used as both
 * a declarator or a selector, depending on how it's used.
 * <p/>
 * Or For example; <br/>
 * <code>Fields fields = new Fields( 1, 2, -1 );</code><br/>
 * This creates a new Fields instance that can only be used as a selector. It would select the second, third, and last
 * position from a given Tuple instance, assuming it has at least four positions. Since the original field names for those
 * positions will carry over to the new selected Tuple instance, if the original Tuple only had three positions, the third
 * and last positions would be the same, and would throw an error on there being duplicate field names in the selected
 * Tuple instance.
 * <p/>
 * Additionally, there are eight predefined Fields sets used for different purposes; {@link #NONE}, {@link #ALL}, {@link #GROUP},
 * {@link #VALUES}, {@link #ARGS}, {@link #RESULTS}, {@link #UNKNOWN}, {@link #REPLACE}, and {@link #SWAP}.
 * <p/>
 * The {@code NONE} Fields set represents no fields.
 * <p/>
 * The {@code ALL} Fields set is a "wildcard" that represents all the current available fields.
 * <p/>
 * The {@code GROUP} Fields set represents all the fields used as grouping values in a previous {@link cascading.pipe.Splice}.
 * If there is no previous Group in the pipe assembly, the GROUP represents all the current field names.
 * <p/>
 * The {@code VALUES} Fields set represent all the fields not used as grouping fields in a previous Group.
 * <p/>
 * The {@code ARGS} Fields set is used to let a given Operation inherit the field names of its argument Tuple. This Fields set
 * is a convenience and is typically used when the Pipe output selector is {@code RESULTS} or {@code REPLACE}.
 * <p/>
 * The {@code RESULTS} Fields set is used to represent the field names of the current Operations return values. This Fields
 * set may only be used as an output selector on a Pipe. It effectively replaces in the input Tuple with the Operation result
 * Tuple.
 * <p/>
 * The {@code UNKNOWN} Fields set is used when Fields must be declared, but how many and their names is unknown. This allows
 * for arbitrarily length Tuples from an input source or some Operation. Use this Fields set with caution.
 * <p/>
 * The {@code REPLACE} Fields set is used as an output selector to inline replace values in the incoming Tuple with
 * the results of an Operation. This is a convenience Fields set that allows subsequent Operations to 'step' on the
 * value with a given field name. The current Operation must always use the exact same field names, or the {@code ARGS}
 * Fields set.
 * <p/>
 * The {@code SWAP} Fields set is used as an output selector to swap out Operation arguments with its results. Neither
 * the argument and result field names or size need to be the same. This is useful for when the Operation arguments are
 * no longer necessary and the result Fields and values should be appended to the remainder of the input field names
 * and Tuple.
 */
public class Fields implements Comparable, Iterable<Comparable>, Serializable, Comparator<Tuple>
  {
  /** Field UNKNOWN */
  public static final Fields UNKNOWN = new Fields( Kind.UNKNOWN );
  /** Field NONE represents a wildcard for no fields */
  public static final Fields NONE = new Fields( Kind.NONE );
  /** Field ALL represents a wildcard for all fields */
  public static final Fields ALL = new Fields( Kind.ALL );
  /** Field KEYS represents all fields used as they key for the last grouping */
  public static final Fields GROUP = new Fields( Kind.GROUP );
  /** Field VALUES represents all fields used as values for the last grouping */
  public static final Fields VALUES = new Fields( Kind.VALUES );
  /** Field ARGS represents all fields used as the arguments for the current operation */
  public static final Fields ARGS = new Fields( Kind.ARGS );
  /** Field RESULTS represents all fields returned by the current operation */
  public static final Fields RESULTS = new Fields( Kind.RESULTS );
  /** Field REPLACE represents all incoming fields, and allows their values to be replaced by the current operation results. */
  public static final Fields REPLACE = new Fields( Kind.REPLACE );
  /** Field SWAP represents all fields not used as arguments for the current operation and the operations results. */
  public static final Fields SWAP = new Fields( Kind.SWAP );
  /** Field FIRST represents the first field position, 0 */
  public static final Fields FIRST = new Fields( 0 );
  /** Field LAST represents the last field position, -1 */
  public static final Fields LAST = new Fields( -1 );

  /** Field EMPTY_INT */
  private static final int[] EMPTY_INT = new int[ 0 ];

  /**
   */
  static enum Kind
    {
      NONE, ALL, GROUP, VALUES, ARGS, RESULTS, UNKNOWN, REPLACE, SWAP
    }

  /** Field fields */
  Comparable[] fields = new Comparable[ 0 ];
  /** Field isOrdered */
  boolean isOrdered = true;
  /** Field kind */
  Kind kind;

  /** Field types */
  Type[] types;
  /** Field comparators */
  Comparator[] comparators;

  /** Field thisPos */
  transient int[] thisPos;
  /** Field index */
  transient Map<Comparable, Integer> index;
  /** Field posCache */
  transient Map<Fields, int[]> posCache;
  /** Field hashCode */
  transient int hashCode; // need to cache this

  /**
   * Method fields is a convenience method to create an array of Fields instances.
   *
   * @param fields of type Fields
   * @return Fields[]
   */
  public static Fields[] fields( Fields... fields )
    {
    return fields;
    }

  public static Comparable[] names( Comparable... names )
    {
    return names;
    }

  public static Type[] types( Type... types )
    {
    return types;
    }

  /**
   * Method size is a factory that makes new instances of Fields the given size.
   *
   * @param size of type int
   * @return Fields
   */
  public static Fields size( int size )
    {
    if( size == 0 )
      return Fields.NONE;

    Fields fields = new Fields();

    fields.fields = expand( size, 0 );

    return fields;
    }

  /**
   * Method join joins all given Fields instances into a new Fields instance.
   * <p/>
   * Use caution with this method, it does not assume the given Fields are either selectors or declarators. Numeric position fields are left untouched.
   * <p/>
   * If the resulting set of fields and ordinals is length zero, {@link Fields#NONE} will be returned.
   *
   * @param fields of type Fields
   * @return Fields
   */
  public static Fields join( Fields... fields )
    {
    return join( false, fields );
    }

  public static Fields join( boolean maskDuplicateNames, Fields... fields )
    {
    int size = 0;

    for( Fields field : fields )
      {
      if( field.isSubstitution() || field.isUnknown() )
        throw new TupleException( "cannot join fields if one is a substitution or is unknown" );

      size += field.size();
      }

    if( size == 0 )
      return Fields.NONE;

    Comparable[] elements = join( size, fields );

    if( maskDuplicateNames )
      {
      Set<String> names = new HashSet<String>();

      for( int i = elements.length - 1; i >= 0; i-- )
        {
        Comparable element = elements[ i ];

        if( names.contains( element ) )
          elements[ i ] = i;
        else if( element instanceof String )
          names.add( (String) element );
        }
      }

    Type[] types = joinTypes( size, fields );

    if( types == null )
      return new Fields( elements );
    else
      return new Fields( elements, types );
    }

  private static Comparable[] join( int size, Fields... fields )
    {
    Comparable[] elements = expand( size, 0 );

    int pos = 0;
    for( Fields field : fields )
      {
      System.arraycopy( field.fields, 0, elements, pos, field.size() );
      pos += field.size();
      }

    return elements;
    }

  private static Type[] joinTypes( int size, Fields... fields )
    {
    Type[] elements = new Type[ size ];

    int pos = 0;
    for( Fields field : fields )
      {
      if( field.isNone() )
        continue;

      if( field.types == null )
        return null;

      System.arraycopy( field.types, 0, elements, pos, field.size() );
      pos += field.size();
      }

    return elements;
    }

  public static Fields mask( Fields fields, Fields mask )
    {
    Comparable[] elements = expand( fields.size(), 0 );

    System.arraycopy( fields.fields, 0, elements, 0, elements.length );

    for( int i = elements.length - 1; i >= 0; i-- )
      {
      Comparable element = elements[ i ];

      if( element instanceof Integer )
        continue;

      if( mask.getIndex().containsKey( element ) )
        elements[ i ] = i;
      }

    return new Fields( elements );
    }

  /**
   * Method merge merges all given Fields instances into a new Fields instance where a merge is a set union of all the
   * given Fields instances.
   * <p/>
   * Thus duplicate positions or field names are allowed, they are subsequently discarded in favor of the first
   * occurrence. That is, merging "a" and "a" would yield "a", not "a, a", yet merging "a,b" and "c" would yield "a,b,c".
   * <p/>
   * Use caution with this method, it does not assume the given Fields are either selectors or declarators. Numeric position fields are left untouched.
   *
   * @param fields of type Fields
   * @return Fields
   */
  public static Fields merge( Fields... fields )
    {
    List<Comparable> elements = new ArrayList<Comparable>();

    for( Fields field : fields )
      {
      for( Comparable comparable : field )
        {
        if( !elements.contains( comparable ) )
          elements.add( comparable );
        }
      }

    return new Fields( elements.toArray( new Comparable[ elements.size() ] ) );
    }

  public static Fields copyComparators( Fields toFields, Fields... fromFields )
    {
    for( Fields fromField : fromFields )
      {
      for( Comparable field : fromField )
        {
        Comparator comparator = fromField.getComparator( field );

        if( comparator != null )
          toFields.setComparator( field, comparator );
        }
      }

    return toFields;
    }

  /**
   * Method offsetSelector is a factory that makes new instances of Fields the given size but offset by startPos.
   * The result Fields instance can only be used as a selector.
   *
   * @param size     of type int
   * @param startPos of type int
   * @return Fields
   */
  public static Fields offsetSelector( int size, int startPos )
    {
    Fields fields = new Fields();

    fields.isOrdered = false;
    fields.fields = expand( size, startPos );

    return fields;
    }

  private static Comparable[] expand( int size, int startPos )
    {
    if( size < 1 )
      throw new TupleException( "invalid size for fields: " + size );

    if( startPos < 0 )
      throw new TupleException( "invalid start position for fields: " + startPos );

    Comparable[] fields = new Comparable[ size ];

    for( int i = 0; i < fields.length; i++ )
      fields[ i ] = i + startPos;

    return fields;
    }

  /**
   * Method resolve returns a new selector expanded on the given field declarations
   *
   * @param selector of type Fields
   * @param fields   of type Fields
   * @return Fields
   */
  public static Fields resolve( Fields selector, Fields... fields )
    {
    boolean hasUnknowns = false;
    int size = 0;

    for( Fields field : fields )
      {
      if( field.isUnknown() )
        hasUnknowns = true;

      if( !field.isDefined() && field.isUnOrdered() )
        throw new TupleException( "unable to select from field set: " + field.printVerbose() );

      size += field.size();
      }

    if( selector.isAll() )
      {
      Fields result = fields[ 0 ];

      for( int i = 1; i < fields.length; i++ )
        result = result.append( fields[ i ] );

      return result;
      }

    if( selector.isReplace() )
      {
      if( fields[ 1 ].isUnknown() )
        throw new TupleException( "cannot replace fields with unknown field declaration" );

      if( !fields[ 0 ].contains( fields[ 1 ] ) )
        throw new TupleException( "could not find all fields to be replaced, available: " + fields[ 0 ].printVerbose() + ",  declared: " + fields[ 1 ].printVerbose() );

      Type[] types = fields[ 0 ].getTypes();

      if( types != null )
        {
        for( int i = 1; i < fields.length; i++ )
          {
          Type[] fieldTypes = fields[ i ].getTypes();
          if( fieldTypes == null )
            continue;

          for( int j = 0; j < fieldTypes.length; j++ )
            fields[ 0 ] = fields[ 0 ].applyType( fields[ i ].get( j ), fieldTypes[ j ] );
          }
        }

      return fields[ 0 ];
      }

    // we can't deal with anything but ALL
    if( !selector.isDefined() )
      throw new TupleException( "unable to use given selector: " + selector );

    Set<String> notFound = new LinkedHashSet<String>();
    Set<String> found = new HashSet<String>();
    Fields result = size( selector.size() );

    if( hasUnknowns )
      size = -1;

    Type[] types = null;

    if( size != -1 )
      types = new Type[ size ];

    int offset = 0;
    for( Fields current : fields )
      {
      if( current.isNone() )
        continue;

      resolveInto( notFound, found, selector, current, result, types, offset, size );
      offset += current.size();
      }

    if( types != null && !Util.containsNull( types ) ) // don't apply types if any are null
      result = result.applyTypes( types );

    notFound.removeAll( found );

    if( !notFound.isEmpty() )
      throw new FieldsResolverException( new Fields( join( size, fields ) ), new Fields( notFound.toArray( new Comparable[ notFound.size() ] ) ) );

    if( hasUnknowns )
      return selector;

    return result;
    }

  private static void resolveInto( Set<String> notFound, Set<String> found, Fields selector, Fields current, Fields result, Type[] types, int offset, int size )
    {
    for( int i = 0; i < selector.size(); i++ )
      {
      Comparable field = selector.get( i );

      if( field instanceof String )
        {
        int index = current.indexOfSafe( field );

        if( index == -1 )
          notFound.add( (String) field );
        else
          result.set( i, handleFound( found, field ) );

        if( index != -1 && types != null && current.getType( index ) != null )
          types[ i ] = current.getType( index );

        continue;
        }

      int pos = current.translatePos( (Integer) field, size ) - offset;

      if( pos >= current.size() || pos < 0 )
        continue;

      Comparable thisField = current.get( pos );

      if( types != null && current.getType( pos ) != null )
        types[ i ] = current.getType( pos );

      if( thisField instanceof String )
        result.set( i, handleFound( found, thisField ) );
      else
        result.set( i, field );
      }
    }

  private static Comparable handleFound( Set<String> found, Comparable field )
    {
    if( found.contains( field ) )
      throw new TupleException( "field name already exists: " + field );

    found.add( (String) field );

    return field;
    }

  /**
   * Method asDeclaration returns a new Fields instance for use as a declarator based on the given fields value.
   * <p/>
   * Typically this is used to convert a selector to a declarator. Simply, all numeric position fields are replaced
   * by their absolute position.
   * <p/>
   * Comparators are preserved in the result.
   *
   * @param fields of type Fields
   * @return Fields
   */
  public static Fields asDeclaration( Fields fields )
    {
    if( fields == null )
      return null;

    if( fields.isNone() )
      return fields;

    if( !fields.isDefined() )
      return UNKNOWN;

    if( fields.isOrdered() )
      return fields;

    Fields result = size( fields.size() );

    copy( null, result, fields, 0 );

    result.types = copyTypes( fields.types, result.size() );
    result.comparators = fields.comparators;

    return result;
    }

  private static Fields asSelector( Fields fields )
    {
    if( !fields.isDefined() )
      return UNKNOWN;

    return fields;
    }

  private Fields()
    {
    }

  /**
   * Constructor Fields creates a new Fields instance.
   *
   * @param kind of type Kind
   */
  @SuppressWarnings({"SameParameterValue"})
  protected Fields( Kind kind )
    {
    this.kind = kind;
    }

  /**
   * Constructor Fields creates a new Fields instance.
   *
   * @param fields of type Comparable...
   */
  @ConstructorProperties({"fields"})
  public Fields( Comparable... fields )
    {
    if( fields.length == 0 )
      this.kind = Kind.NONE;
    else
      this.fields = validate( fields );
    }

  public Fields( Comparable field, Type type )
    {
    this( names( field ), types( type ) );
    }

  public Fields( Comparable[] fields, Type[] types )
    {
    this( fields );

    if( isDefined() && types != null )
      {
      if( this.fields.length != types.length )
        throw new IllegalArgumentException( "given types array must be same length as fields" );

      this.types = copyTypes( types, this.fields.length );
      }
    }

  /**
   * Method isUnOrdered returns true if this instance is unordered. That is, it has relative numeric field positions.
   * For example; [1,"a",2,-1]
   *
   * @return the unOrdered (type boolean) of this Fields object.
   */
  public boolean isUnOrdered()
    {
    return !isOrdered || kind == Kind.ALL;
    }

  /**
   * Method isOrdered returns true if this instance is ordered. That is, all numeric field positions are absolute.
   * For example; [0,"a",2,3]
   *
   * @return the ordered (type boolean) of this Fields object.
   */
  public boolean isOrdered()
    {
    return isOrdered || kind == Kind.UNKNOWN;
    }

  /**
   * Method isDefined returns true if this instance is not a field set like {@link #ALL} or {@link #UNKNOWN}.
   *
   * @return the defined (type boolean) of this Fields object.
   */
  public boolean isDefined()
    {
    return kind == null;
    }

  /**
   * Method isOutSelector returns true if this instance is 'defined', or the field set {@link #ALL} or {@link #RESULTS}.
   *
   * @return the outSelector (type boolean) of this Fields object.
   */
  public boolean isOutSelector()
    {
    return isAll() || isResults() || isReplace() || isSwap() || isDefined();
    }

  /**
   * Method isArgSelector returns true if this instance is 'defined' or the field set {@link #ALL}, {@link #GROUP}, or
   * {@link #VALUES}.
   *
   * @return the argSelector (type boolean) of this Fields object.
   */
  public boolean isArgSelector()
    {
    return isAll() || isNone() || isGroup() || isValues() || isDefined();
    }

  /**
   * Method isDeclarator returns true if this can be used as a declarator. Specifically if it is 'defined' or
   * {@link #UNKNOWN}, {@link #ALL}, {@link #ARGS}, {@link #GROUP}, or {@link #VALUES}.
   *
   * @return the declarator (type boolean) of this Fields object.
   */
  public boolean isDeclarator()
    {
    return isUnknown() || isNone() || isAll() || isArguments() || isGroup() || isValues() || isDefined();
    }

  /**
   * Method isNone returns returns true if this instance is the {@link #NONE} field set.
   *
   * @return the none (type boolean) of this Fields object.
   */
  public boolean isNone()
    {
    return kind == Kind.NONE;
    }

  /**
   * Method isAll returns true if this instance is the {@link #ALL} field set.
   *
   * @return the all (type boolean) of this Fields object.
   */
  public boolean isAll()
    {
    return kind == Kind.ALL;
    }

  /**
   * Method isUnknown returns true if this instance is the {@link #UNKNOWN} field set.
   *
   * @return the unknown (type boolean) of this Fields object.
   */
  public boolean isUnknown()
    {
    return kind == Kind.UNKNOWN;
    }

  /**
   * Method isArguments returns true if this instance is the {@link #ARGS} field set.
   *
   * @return the arguments (type boolean) of this Fields object.
   */
  public boolean isArguments()
    {
    return kind == Kind.ARGS;
    }

  /**
   * Method isValues returns true if this instance is the {@link #VALUES} field set.
   *
   * @return the values (type boolean) of this Fields object.
   */
  public boolean isValues()
    {
    return kind == Kind.VALUES;
    }

  /**
   * Method isResults returns true if this instance is the {@link #RESULTS} field set.
   *
   * @return the results (type boolean) of this Fields object.
   */
  public boolean isResults()
    {
    return kind == Kind.RESULTS;
    }

  /**
   * Method isReplace returns true if this instance is the {@link #REPLACE} field set.
   *
   * @return the replace (type boolean) of this Fields object.
   */
  public boolean isReplace()
    {
    return kind == Kind.REPLACE;
    }

  /**
   * Method isSwap returns true if this instance is the {@link #SWAP} field set.
   *
   * @return the swap (type boolean) of this Fields object.
   */
  public boolean isSwap()
    {
    return kind == Kind.SWAP;
    }

  /**
   * Method isKeys returns true if this instance is the {@link #GROUP} field set.
   *
   * @return the keys (type boolean) of this Fields object.
   */
  public boolean isGroup()
    {
    return kind == Kind.GROUP;
    }

  /**
   * Method isSubstitution returns true if this instance is a substitution fields set. Specifically if it is the field
   * set {@link #ALL}, {@link #ARGS}, {@link #GROUP}, or {@link #VALUES}.
   *
   * @return the substitution (type boolean) of this Fields object.
   */
  public boolean isSubstitution()
    {
    return isAll() || isArguments() || isGroup() || isValues();
    }

  private Comparable[] validate( Comparable[] fields )
    {
    isOrdered = true;

    Set<Comparable> names = new HashSet<Comparable>();

    for( int i = 0; i < fields.length; i++ )
      {
      Comparable field = fields[ i ];

      if( !( field instanceof String || field instanceof Integer ) )
        throw new IllegalArgumentException( String.format( "invalid field type (%s); must be String or Integer: ", field ) );

      if( names.contains( field ) )
        throw new IllegalArgumentException( "duplicate field name found: " + field );

      names.add( field );

      if( field instanceof Number && (Integer) field != i )
        isOrdered = false;
      }

    return fields;
    }

  final Comparable[] get()
    {
    return fields;
    }

  /**
   * Method get returns the field name or position at the given index i.
   *
   * @param i is of type int
   * @return Comparable
   */
  public final Comparable get( int i )
    {
    return fields[ i ];
    }

  final void set( int i, Comparable comparable )
    {
    fields[ i ] = comparable;

    if( isOrdered() && comparable instanceof Integer )
      isOrdered = i == (Integer) comparable;
    }

  /**
   * Method getPos returns the pos array of this Fields object.
   *
   * @return the pos (type int[]) of this Fields object.
   */
  public int[] getPos()
    {
    if( thisPos != null )
      return thisPos; // do not clone

    if( isAll() || isUnknown() )
      thisPos = EMPTY_INT;
    else
      thisPos = makeThisPos();

    return thisPos;
    }

  private int[] makeThisPos()
    {
    int[] pos = new int[ size() ];

    for( int i = 0; i < size(); i++ )
      {
      Comparable field = get( i );

      if( field instanceof Number )
        pos[ i ] = (Integer) field;
      else
        pos[ i ] = i;
      }

    return pos;
    }

  private final Map<Fields, int[]> getPosCache()
    {
    if( posCache == null )
      posCache = new HashMap<Fields, int[]>();

    return posCache;
    }

  private final int[] putReturn( Fields fields, int[] pos )
    {
    getPosCache().put( fields, pos );

    return pos;
    }

  public final int[] getPos( Fields fields )
    {
    return getPos( fields, -1 );
    }

  final int[] getPos( Fields fields, int tupleSize )
    {
    // test for key, as we stuff a null value
    if( !isUnknown() && getPosCache().containsKey( fields ) )
      return getPosCache().get( fields );

    if( fields.isAll() )
      return putReturn( fields, null ); // return null, not getPos()

    if( isAll() )
      return putReturn( fields, fields.getPos() );

    // don't cache unknown
    if( size() == 0 && isUnknown() )
      return translatePos( fields, tupleSize );

    int[] pos = translatePos( fields, size() );

    return putReturn( fields, pos );
    }

  private int[] translatePos( Fields fields, int fieldSize )
    {
    int[] pos = new int[ fields.size() ];

    for( int i = 0; i < fields.size(); i++ )
      {
      Comparable field = fields.get( i );

      if( field instanceof Number )
        pos[ i ] = translatePos( (Integer) field, fieldSize );
      else
        pos[ i ] = indexOf( field );
      }

    return pos;
    }

  final int translatePos( Integer integer )
    {
    return translatePos( integer, size() );
    }

  final int translatePos( Integer integer, int size )
    {
    if( size == -1 )
      return integer;

    if( integer < 0 )
      integer = size + integer;

    if( !isUnknown() && ( integer >= size || integer < 0 ) )
      throw new TupleException( "position value is too large: " + integer + ", positions in field: " + size );

    return integer;
    }

  /**
   * Method getPos returns the index of the give field value in this Fields instance. The index corresponds to the
   * Tuple value index in an associated Tuple instance.
   *
   * @param fieldName of type Comparable
   * @return int
   */
  public int getPos( Comparable fieldName )
    {
    if( fieldName instanceof Number )
      return translatePos( (Integer) fieldName );
    else
      return indexOf( fieldName );
    }

  private final Map<Comparable, Integer> getIndex()
    {
    if( index != null )
      return index;

    // make thread-safe by not having invalid intermediate state
    Map<Comparable, Integer> local = new HashMap<Comparable, Integer>();

    for( int i = 0; i < size(); i++ )
      local.put( get( i ), i );

    return index = local;
    }

  private int indexOf( Comparable fieldName )
    {
    Integer result = getIndex().get( fieldName );

    if( result == null )
      throw new FieldsResolverException( this, new Fields( fieldName ) );

    return result;
    }

  int indexOfSafe( Comparable fieldName )
    {
    Integer result = getIndex().get( fieldName );

    if( result == null )
      return -1;

    return result;
    }

  /**
   * Method iterator return an unmodifiable iterator of field values. if {@link #isSubstitution()} returns true,
   * this iterator will be empty.
   *
   * @return Iterator
   */
  public Iterator iterator()
    {
    return Collections.unmodifiableList( Arrays.asList( fields ) ).iterator();
    }

  /**
   * Method select returns a new Fields instance with fields specified by the given selector.
   *
   * @param selector of type Fields
   * @return Fields
   */
  public Fields select( Fields selector )
    {
    if( !isOrdered() )
      throw new TupleException( "this fields instance can only be used as a selector" );

    if( selector.isAll() )
      return this;

    // supports -1_UNKNOWN_RETURNED
    // guarantees pos arguments remain selector positions, not absolute positions
    if( isUnknown() )
      return asSelector( selector );

    if( selector.isNone() )
      return NONE;

    Fields result = size( selector.size() );

    for( int i = 0; i < selector.size(); i++ )
      {
      Comparable field = selector.get( i );

      if( field instanceof String )
        {
        result.set( i, get( indexOf( field ) ) );
        continue;
        }

      int pos = translatePos( (Integer) field );

      if( this.get( pos ) instanceof String )
        result.set( i, this.get( pos ) );
      else
        result.set( i, pos ); // use absolute position if no field name
      }

    if( this.types != null )
      {
      result.types = new Type[ result.size() ];

      for( int i = 0; i < selector.size(); i++ )
        {
        Comparable field = selector.get( i );

        if( field instanceof String )
          result.setType( i, getType( indexOf( field ) ) );
        else
          result.setType( i, getType( translatePos( (Integer) field ) ) );
        }
      }

    return result;
    }

  /**
   * Method selectPos returns a Fields instance with only positional fields, no field names.
   *
   * @param selector of type Fields
   * @return Fields instance with only positions.
   */
  public Fields selectPos( Fields selector )
    {
    return selectPos( selector, 0 );
    }

  /**
   * Method selectPos returns a Fields instance with only positional fields, offset by given offset value, no field names.
   *
   * @param selector of type Fields
   * @param offset   of type int
   * @return Fields instance with only positions.
   */
  public Fields selectPos( Fields selector, int offset )
    {
    int[] pos = getPos( selector );

    Fields results = size( pos.length );

    for( int i = 0; i < pos.length; i++ )
      results.fields[ i ] = pos[ i ] + offset;

    return results;
    }

  /**
   * Method subtract returns the difference between this instance and the given fields instance.
   * <p/>
   * See {@link #append(Fields)} for adding field names.
   *
   * @param fields of type Fields
   * @return Fields
   */
  public Fields subtract( Fields fields )
    {
    if( fields.isAll() )
      return Fields.NONE;

    if( fields.isNone() )
      return this;

    List<Comparable> list = new LinkedList<Comparable>();
    Collections.addAll( list, this.get() );
    int[] pos = getPos( fields, -1 );

    for( int i : pos )
      list.set( i, null );

    Util.removeAllNulls( list );

    Type[] newTypes = null;

    if( this.types != null )
      {
      List<Type> types = new LinkedList<Type>();
      Collections.addAll( types, this.types );

      for( int i : pos )
        types.set( i, null );

      Util.removeAllNulls( types );

      newTypes = types.toArray( new Type[ types.size() ] );
      }

    return new Fields( list.toArray( new Comparable[ list.size() ] ), newTypes );
    }

  /**
   * Method is used for appending the given Fields instance to this instance, into a new Fields instance.
   * <p/>
   * See {@link #subtract(Fields)} for removing field names.
   * <p/>
   * This method has been deprecated, see {@link #join(Fields...)}
   *
   * @param fields of type Fields[]
   * @return Fields
   */
  @Deprecated
  public Fields append( Fields[] fields )
    {
    if( fields.length == 0 )
      return null;

    Fields field = this;

    for( Fields current : fields )
      field = field.append( current );

    return field;
    }

  /**
   * Method is used for appending the given Fields instance to this instance, into a new Fields instance.
   * <p/>
   * Note any relative positional elements are retained, thus appending two Fields each declaring {@code -1}
   * position will result in a TupleException noting duplicate fields.
   * <p/>
   * See {@link #subtract(Fields)} for removing field names.
   *
   * @param fields of type Fields
   * @return Fields
   */
  public Fields append( Fields fields )
    {
    if( fields == null )
      return this;

    // allow unordered fields to be appended to build more complex selectors
    if( this.isAll() || fields.isAll() )
      throw new TupleException( "cannot append fields: " + this.print() + " + " + fields.print() );

    if( ( this.isUnknown() || this.size() == 0 ) && fields.isUnknown() )
      return UNKNOWN;

    if( fields.isNone() )
      return this;

    if( this.isNone() )
      return fields;

    Set<Comparable> names = new HashSet<Comparable>();

    // init the Field
    Fields result = size( this.size() + fields.size() );

    // copy over field names from this side
    copyRetain( names, result, this, 0 );
    // copy over field names from that side
    copyRetain( names, result, fields, this.size() );

    if( this.isUnknown() || fields.isUnknown() )
      result.kind = Kind.UNKNOWN;

    if( ( this.isNone() || this.types != null ) && fields.types != null )
      {
      result.types = new Type[ this.size() + fields.size() ];

      if( this.types != null ) // supports appending to NONE
        System.arraycopy( this.types, 0, result.types, 0, this.size() );

      System.arraycopy( fields.types, 0, result.types, this.size(), fields.size() );
      }

    return result;
    }

  /**
   * Method rename will rename the from fields to the values in to to fields. Fields may contain field names, or
   * positions.
   * <p/>
   * Using positions is useful to remove a field name put keep its place in the Tuple stream.
   *
   * @param from of type Fields
   * @param to   of type Fields
   * @return Fields
   */
  public Fields rename( Fields from, Fields to )
    {
    if( this.isSubstitution() || this.isUnknown() )
      throw new TupleException( "cannot rename fields in a substitution or unknown Fields instance: " + this.print() );

    if( from.size() != to.size() )
      throw new TupleException( "from and to fields must be the same size" );

    if( from.isSubstitution() || from.isUnknown() )
      throw new TupleException( "from fields may not be a substitution or unknown" );

    if( to.isSubstitution() || to.isUnknown() )
      throw new TupleException( "to fields may not be a substitution or unknown" );

    Comparable[] newFields = Arrays.copyOf( this.fields, this.fields.length );

    int[] pos = getPos( from );

    for( int i = 0; i < pos.length; i++ )
      newFields[ pos[ i ] ] = to.fields[ i ];

    Type[] newTypes = null;

    if( this.types != null && to.types != null )
      {
      newTypes = copyTypes( this.types, this.size() );

      for( int i = 0; i < pos.length; i++ )
        newTypes[ pos[ i ] ] = to.types[ i ];
      }

    return new Fields( newFields, newTypes );
    }

  /**
   * Method project will return a new Fields instance similar to the given fields instance
   * except any absolute positional elements will be replaced by the current field names, if any.
   *
   * @param fields of type Fields
   * @return Fields
   */
  public Fields project( Fields fields )
    {
    if( fields == null )
      return this;

    Fields results = size( fields.size() ).applyTypes( fields.getTypes() );

    for( int i = 0; i < fields.fields.length; i++ )
      {
      if( fields.fields[ i ] instanceof String )
        results.fields[ i ] = fields.fields[ i ];
      else if( this.fields[ i ] instanceof String )
        results.fields[ i ] = this.fields[ i ];
      else
        results.fields[ i ] = i;
      }

    return results;
    }

  private static void copy( Set<String> names, Fields result, Fields fields, int offset )
    {
    for( int i = 0; i < fields.size(); i++ )
      {
      Comparable field = fields.get( i );

      if( !( field instanceof String ) )
        continue;

      if( names != null )
        {
        if( names.contains( field ) )
          throw new TupleException( "field name already exists: " + field );

        names.add( (String) field );
        }

      result.set( i + offset, field );
      }
    }

  /**
   * Retains any relative positional elements like -1, but checks for duplicates
   *
   * @param names
   * @param result
   * @param fields
   * @param offset
   */
  private static void copyRetain( Set<Comparable> names, Fields result, Fields fields, int offset )
    {
    for( int i = 0; i < fields.size(); i++ )
      {
      Comparable field = fields.get( i );

      if( field instanceof Integer && ( (Integer) field ) > -1 )
        continue;

      if( names != null )
        {
        if( names.contains( field ) )
          throw new TupleException( "field name already exists: " + field );

        names.add( field );
        }

      result.set( i + offset, field );
      }
    }

  /**
   * Method verifyContains tests if this instance contains the field names and positions specified in the given
   * fields instance. If the test fails, a {@link TupleException} is thrown.
   *
   * @param fields of type Fields
   * @throws TupleException when one or more fields are not contained in this instance.
   */
  public void verifyContains( Fields fields )
    {
    if( isUnknown() )
      return;

    try
      {
      getPos( fields );
      }
    catch( TupleException exception )
      {
      throw new TupleException( "these fields " + print() + ", do not contain " + fields.print() );
      }
    }

  /**
   * Method contains returns true if this instance contains the field names and positions specified in the given
   * fields instance.
   *
   * @param fields of type Fields
   * @return boolean
   */
  public boolean contains( Fields fields )
    {
    try
      {
      getPos( fields );
      return true;
      }
    catch( Exception exception )
      {
      return false;
      }
    }

  /**
   * Method compareTo compares this instance to the given Fields instance.
   *
   * @param other of type Fields
   * @return int
   */
  public int compareTo( Fields other )
    {
    if( other.size() != size() )
      return other.size() < size() ? 1 : -1;

    for( int i = 0; i < size(); i++ )
      {
      int c = get( i ).compareTo( other.get( i ) );

      if( c != 0 )
        return c;
      }

    return 0;
    }

  /**
   * Method compareTo implements {@link Comparable#compareTo(Object)}.
   *
   * @param other of type Object
   * @return int
   */
  public int compareTo( Object other )
    {
    if( other instanceof Fields )
      return compareTo( (Fields) other );
    else
      return -1;
    }

  /**
   * Method print returns a String representation of this instance.
   *
   * @return String
   */
  public String print()
    {
    return "[" + toString() + "]";
    }

  /**
   * Method printLong returns a String representation of this instance along with the size.
   *
   * @return String
   */
  public String printVerbose()
    {
    String fieldsString = toString();

    if( types != null )
      fieldsString += " | " + Util.join( Util.typeNames( types ), ", " );

    return "[{" + ( isDefined() ? size() : "?" ) + "}:" + fieldsString + "]";
    }


  @Override
  public String toString()
    {
    if( isOrdered() )
      return orderedToString();
    else
      return unorderedToString();
    }

  private String orderedToString()
    {
    StringBuffer buffer = new StringBuffer();

    if( size() != 0 )
      {
      int startIndex = get( 0 ) instanceof Number ? (Integer) get( 0 ) : 0;

      for( int i = 0; i < size(); i++ )
        {
        Comparable field = get( i );

        if( field instanceof Number )
          {
          if( i + 1 == size() || !( get( i + 1 ) instanceof Number ) )
            {
            if( buffer.length() != 0 )
              buffer.append( ", " );

            if( startIndex != i )
              buffer.append( startIndex ).append( ":" ).append( field );
            else
              buffer.append( i );

            startIndex = i;
            }

          continue;
          }

        if( i != 0 )
          buffer.append( ", " );

        if( field instanceof String )
          buffer.append( "\'" ).append( field ).append( "\'" );
        else if( field instanceof Fields )
          buffer.append( ( (Fields) field ).print() );

        startIndex = i + 1;
        }
      }

    if( kind != null )
      {
      if( buffer.length() != 0 )
        buffer.append( ", " );
      buffer.append( kind );
      }

    return buffer.toString();
    }

  private String unorderedToString()
    {
    StringBuffer buffer = new StringBuffer();

    for( Object field : get() )
      {
      if( buffer.length() != 0 )
        buffer.append( ", " );

      if( field instanceof String )
        buffer.append( "\'" ).append( field ).append( "\'" );
      else if( field instanceof Fields )
        buffer.append( ( (Fields) field ).print() );
      else
        buffer.append( field );
      }

    if( kind != null )
      {
      if( buffer.length() != 0 )
        buffer.append( ", " );
      buffer.append( kind );
      }

    return buffer.toString();
    }

  /**
   * Method size returns the number of field positions in this instance.
   *
   * @return int
   */
  public final int size()
    {
    return fields.length;
    }

  /**
   * Method applyType should be used to associate a {@link java.lang.reflect.Type} with a given field name or position.
   * A new instance of Fields will be returned, this instance will not be modified.
   * <p/>
   * {@code fieldName} may optionally be a {@link Fields} instance. Only the first field name or position will
   * be considered.
   *
   * @param fieldName of type Comparable
   * @param type      of type Type
   */
  public Fields applyType( Comparable fieldName, Type type )
    {
    if( type == null )
      throw new IllegalArgumentException( "given type must not be null" );

    int pos;

    try
      {
      pos = getPos( asFieldName( fieldName ) );
      }
    catch( FieldsResolverException exception )
      {
      throw new IllegalArgumentException( "given field name was not found: " + fieldName, exception );
      }

    Fields results = new Fields( fields );

    results.types = this.types == null ? new Type[ size() ] : this.types;
    results.types[ pos ] = type;

    return results;
    }

  /**
   * Method applyType should be used to associate {@link java.lang.reflect.Type} with a given field name or position
   * as declared in the given Fields parameter.
   * <p/>
   * A new instance of Fields will be returned, this instance will not be modified.
   * <p/>
   *
   * @param fields of type Fields
   */
  public Fields applyTypes( Fields fields )
    {
    Fields result = new Fields( this.fields, this.types );

    for( Comparable field : fields )
      result = result.applyType( field, fields.getType( fields.getPos( field ) ) );

    return result;
    }

  /**
   * Method applyTypes returns a new Fields instance with the given types, replacing any existing type
   * information within the new instance.
   * <p/>
   * The Class array must be the same length as the number for fields in this instance.
   *
   * @param types the class types of this Fields object.
   * @return returns a new instance of Fields with this instances field names and the given types
   */
  public Fields applyTypes( Type... types )
    {
    Fields result = new Fields( fields );

    if( types == null ) // allows for type erasure
      return result;

    if( types.length != size() )
      throw new IllegalArgumentException( "given number of class instances must match fields size" );

    for( Type type : types )
      {
      if( type == null )
        throw new IllegalArgumentException( "type must not be null" );
      }

    result.types = copyTypes( types, types.length ); // make copy as Class[] could be passed in

    return result;
    }

  /**
   * Returns the Type at the given position or having the fieldName.
   *
   * @param fieldName of type String or Number
   * @return the Type
   */
  public Type getType( Comparable fieldName )
    {
    return getType( getPos( fieldName ) );
    }

  public Type getType( int pos )
    {
    if( !hasTypes() )
      return null;

    return this.types[ pos ];
    }

  /**
   * Returns the Class for the given position value.
   *
   * @param fieldName of type String or Number
   * @return type Class
   */
  public Class getTypeClass( Comparable fieldName )
    {
    return getTypeClass( getPos( fieldName ) );
    }

  public Class getTypeClass( int pos )
    {
    return (Class) getType( pos );
    }

  protected void setType( int pos, Type type )
    {
    if( type == null )
      throw new IllegalArgumentException( "type may not be null" );

    this.types[ pos ] = type;
    }

  /**
   * Returns a copy of the current types Type[] if any, else null.
   *
   * @return of type Type[]
   */
  public Type[] getTypes()
    {
    return copyTypes( types, size() );
    }

  /**
   * Returns a copy of the current types Class[] if any, else null.
   * <p/>
   * May fail if all types are not an instance of {@link Class}.
   *
   * @return of type Class
   */
  public Class[] getTypesClasses()
    {
    if( types == null )
      return null;

    Class[] classes = new Class[ types.length ];

    for( int i = 0; i < types.length; i++ )
      classes[ i ] = (Class) types[ i ]; // this throws a more helpful exception vs arraycopy

    return classes;
    }

  private static Type[] copyTypes( Type[] types, int size )
    {
    if( types == null )
      return null;

    Type[] copy = new Type[ size ];

    if( types.length != size )
      throw new IllegalArgumentException( "types array must be same size as fields array" );

    System.arraycopy( types, 0, copy, 0, size );

    return copy;
    }

  /**
   * Returns true if there are types associated with this instance.
   *
   * @return boolean
   */
  public final boolean hasTypes()
    {
    return types != null;
    }

  /**
   * Method setComparator should be used to associate a {@link java.util.Comparator} with a given field name or position.
   * <p/>
   * {@code fieldName} may optionally be a {@link Fields} instance. Only the first field name or position will
   * be considered.
   *
   * @param fieldName  of type Comparable
   * @param comparator of type Comparator
   */
  public void setComparator( Comparable fieldName, Comparator comparator )
    {
    if( !( comparator instanceof Serializable ) )
      throw new IllegalArgumentException( "given comparator must be serializable" );

    if( comparators == null )
      comparators = new Comparator[ size() ];

    try
      {
      comparators[ getPos( asFieldName( fieldName ) ) ] = comparator;
      }
    catch( FieldsResolverException exception )
      {
      throw new IllegalArgumentException( "given field name was not found: " + fieldName, exception );
      }
    }

  /**
   * Method setComparators sets all the comparators of this Fields object. The Comparator array
   * must be the same length as the number for fields in this instance.
   *
   * @param comparators the comparators of this Fields object.
   */
  public void setComparators( Comparator... comparators )
    {
    if( comparators.length != size() )
      throw new IllegalArgumentException( "given number of comparator instances must match fields size" );

    for( Comparator comparator : comparators )
      {
      if( !( comparator instanceof Serializable ) )
        throw new IllegalArgumentException( "comparators must be serializable" );
      }

    this.comparators = comparators;
    }

  protected static Comparable asFieldName( Comparable fieldName )
    {
    if( fieldName instanceof Fields )
      {
      Fields fields = (Fields) fieldName;

      if( !fields.isDefined() )
        throw new TupleException( "given Fields instance must explicitly declare one field name or position: " + fields.printVerbose() );

      fieldName = fields.get( 0 );
      }

    return fieldName;
    }

  protected Comparator getComparator( Comparable fieldName )
    {
    if( comparators == null )
      return null;

    try
      {
      return comparators[ getPos( asFieldName( fieldName ) ) ];
      }
    catch( FieldsResolverException exception )
      {
      return null;
      }
    }

  /**
   * Method getComparators returns the comparators of this Fields object.
   *
   * @return the comparators (type Comparator[]) of this Fields object.
   */
  public Comparator[] getComparators()
    {
    Comparator[] copy = new Comparator[ size() ];

    if( comparators != null )
      System.arraycopy( comparators, 0, copy, 0, size() );

    return copy;
    }

  /**
   * Method hasComparators test if this Fields instance has Comparators.
   *
   * @return boolean
   */
  public boolean hasComparators()
    {
    return comparators != null;
    }

  @Override
  public int compare( Tuple lhs, Tuple rhs )
    {
    return lhs.compareTo( comparators, rhs );
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( object == null || getClass() != object.getClass() )
      return false;

    Fields fields = (Fields) object;

    return equalsFields( fields ) && Arrays.equals( types, fields.types );
    }

  /**
   * Method equalsFields compares only the internal field names and postions only between this and the given Fields
   * instance. Type information is ignored.
   *
   * @param fields of type int
   * @return true if this and the given instance have the same positions and/or field names.
   */
  public boolean equalsFields( Fields fields )
    {
    return fields != null && this.kind == fields.kind && Arrays.equals( get(), fields.get() );
    }

  @Override
  public int hashCode()
    {
    if( hashCode == 0 )
      hashCode = get() != null ? Arrays.hashCode( get() ) : 0;

    return hashCode;
    }

  }
