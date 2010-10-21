/*
 * Copyright (c) 2007-2010 Concurrent, Inc. All Rights Reserved.
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

import java.beans.ConstructorProperties;
import java.io.Serializable;
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

import cascading.pipe.Group;
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
 * Additionally, there are eight predefined Fields sets used for different purposes; {@link #ALL}, {@link #GROUP},
 * {@link #VALUES}, {@link #ARGS}, {@link #RESULTS}, {@link #UNKNOWN}, {@link #REPLACE}, and {@link #SWAP}.
 * <p/>
 * The {@code ALL} Fields set is a "wildcard" that represents all the current available fields.
 * <p/>
 * The {@code GROUP} Fields set represents all the fields used as grouping values in a previous {@link Group}.
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
  private static final int[] EMPTY_INT = new int[0];

  /**
   */
  static enum Kind
    {
      ALL, GROUP, VALUES, ARGS, RESULTS, UNKNOWN, REPLACE, SWAP;
    }

  /** Field fields */
  Comparable[] fields = new Comparable[0];
  /** Field isOrdered */
  boolean isOrdered = true;
  /** Field kind */
  Kind kind;
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

  /**
   * Method size is a factory that makes new instances of Fields the given size.
   *
   * @param size of type int
   * @return Fields
   */
  public static Fields size( int size )
    {
    Fields fields = new Fields();

    fields.fields = expand( size, 0 );

    return fields;
    }

  /**
   * Method join joins all given Fields instances into a new Fields instance.
   * <p/>
   * Use caution with this method, it does not assume the given Fields are either selectors or declarators. Numeric position fields are left untouched.
   *
   * @param fields of type Fields
   * @return Fields
   */
  public static Fields join( Fields... fields )
    {
    int size = 0;

    for( Fields field : fields )
      {
      if( field.isSubstitution() || field.isUnknown() )
        throw new TupleException( "cannot join fields if one is a substitution or is unknown" );

      size += field.size();
      }

    Comparable[] elements = join( size, fields );

    return new Fields( elements );
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

    Comparable[] fields = new Comparable[size];

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

    int offset = 0;
    for( Fields current : fields )
      {
      resolveInto( notFound, found, selector, current, result, offset, size );
      offset += current.size();
      }

    notFound.removeAll( found );

    if( !notFound.isEmpty() )
      throw new FieldsResolverException( new Fields( join( size, fields ) ), new Fields( notFound.toArray( new Comparable[0] ) ) );

    if( hasUnknowns )
      return selector;

    return result;
    }

  private static void resolveInto( Set<String> notFound, Set<String> found, Fields selector, Fields current, Fields result, int offset, int size )
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

        continue;
        }

      int pos = current.translatePos( (Integer) field, size ) - offset;

      if( pos >= current.size() || pos < 0 )
        continue;

      Comparable thisField = current.get( pos );

      if( thisField instanceof String )
        result.set( i, handleFound( found, thisField ) );
      else
        result.set( i, field );
      }
    }

  private static Comparable handleFound( Set<String> found, Comparable field )
    {
    if( found.contains( (String) field ) )
      throw new TupleException( "field name already exists: " + field );

    found.add( (String) field );

    return field;
    }

  /**
   * Method asDeclaration returns a new Fields instance for use as a declarator based on the given fields value.
   * <p/>
   * Typically this is used to convert a selector to a declarator. Simply, all numeric position fields are replaced
   * by their absolute position.
   *
   * @param fields of type Fields
   * @return Fields
   */
  public static Fields asDeclaration( Fields fields )
    {
    if( !fields.isDefined() )
      return UNKNOWN;

    if( fields.isOrdered() )
      return fields;

    Fields result = size( fields.size() );

    copy( null, result, fields, 0 );

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
    this.fields = validate( fields );
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
   * Method isOrdered returns true if this instance is orderd. That is, all numeric field positions are absolute.
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
    return isAll() || isGroup() || isValues() || isDefined();
    }

  /**
   * Method isDeclarator returns true if this can be used as a declarator. Specifically if it is 'defined' or
   * {@link #UNKNOWN}, {@link #ALL}, {@link #ARGS}, {@link #GROUP}, or {@link #VALUES}.
   *
   * @return the declarator (type boolean) of this Fields object.
   */
  public boolean isDeclarator()
    {
    return isUnknown() || isAll() || isArguments() || isGroup() || isValues() || isDefined();
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

      if( field == null )
        throw new IllegalArgumentException( "field name or position may not be null" );

      if( field instanceof Fields )
        throw new IllegalArgumentException( "may not nest Fields instances within a Fields instance" );

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
    int[] pos = new int[size()];

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

  final int[] getPos( Fields fields )
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
    int[] pos = new int[fields.size()];

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

    Fields result = size( selector.size() );

    // todo: this can be cleaned up i think
    for( int i = 0; i < selector.size(); i++ )
      {
      Comparable field = selector.get( i );

      if( field instanceof String )
        result.set( i, get( indexOf( field ) ) );
      else if( this.get( translatePos( (Integer) field ) ) instanceof String )
        result.set( i, get( translatePos( (Integer) field ) ) );
      else
        result.set( i, translatePos( (Integer) field ) ); // use absolute position if no field name
      }

    return result;
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
    Fields minus = new Fields();

    if( fields.isAll() )
      return minus;

    List<Comparable> list = new LinkedList<Comparable>();
    Collections.addAll( list, this.get() );
    int[] pos = getPos( fields, -1 );

    for( int i : pos )
      list.set( i, null );

    Util.removeAllNulls( list );

    minus.fields = list.toArray( new Comparable[list.size()] );

    return minus;
    }

  /**
   * Method is used for appending the given Fields instance to this instance, into a new Fields instance.
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

    if( this.isAll() || fields.isAll() || !this.isOrdered() || !fields.isOrdered() )
      throw new TupleException( "cannot append fields: " + this.print() + " + " + fields.print() );

    if( ( this.isUnknown() || this.size() == 0 ) && fields.isUnknown() )
      return UNKNOWN;

    Set<String> names = new HashSet<String>();

    // init the Field
    Fields result = size( this.size() + fields.size() );

    // copy over field names from this side
    copy( names, result, this, 0 );
    // copy over field names from that side
    copy( names, result, fields, this.size() );

    if( this.isUnknown() || fields.isUnknown() )
      result.kind = Kind.UNKNOWN;

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
      throw new TupleException( "cannot rename fields in a substitution or unkown Fields instance: " + this.print() );

    if( from.size() != to.size() )
      throw new TupleException( "from and to fields must be the same size" );

    if( from.isSubstitution() || from.isUnknown() )
      throw new TupleException( "from fields may not be a substitution or unknown" );

    if( to.isSubstitution() || to.isUnknown() )
      throw new TupleException( "to fields may not be a substitution or unknown" );

    Set<String> names = new HashSet<String>();

    Comparable[] newFields = Arrays.copyOf( this.fields, this.fields.length );

    int[] pos = getPos( from );

    for( int i = 0; i < pos.length; i++ )
      newFields[ pos[ i ] ] = to.fields[ i ];

    return new Fields( newFields );
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

    Fields results = size( fields.size() );

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
        if( names.contains( (String) field ) )
          throw new TupleException( "field name already exists: " + field );

        names.add( (String) field );
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
    return "[{" + ( isDefined() ? size() : "?" ) + "}:" + toString() + "]";
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
   * Method setComparator should be used to associate a {@link java.util.Comparator} with a given field name or position.
   * <br/>
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
      comparators = new Comparator[size()];

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
   * Method setComparators sets all the comparators of this FieldsComparator object. The Comparator array
   * must be the same length as the number for fields in this instance.
   *
   * @param comparators the comparators of this FieldsComparator object.
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

  private Comparable asFieldName( Comparable fieldName )
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

  /**
   * Method getComparators returns the comparators of this Fields object.
   *
   * @return the comparators (type Comparator[]) of this Fields object.
   */
  public Comparator[] getComparators()
    {
    Comparator[] copy = new Comparator[size()];

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

    Fields fields1 = (Fields) object;

    return this.kind == fields1.kind && Arrays.equals( get(), fields1.get() );
    }

  @Override
  public int hashCode()
    {
    if( hashCode == 0 )
      hashCode = get() != null ? Arrays.hashCode( get() ) : 0;

    return hashCode;
    }

  }
