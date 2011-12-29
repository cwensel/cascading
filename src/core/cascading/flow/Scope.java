/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/** Class Scope is an internal representation of the linkages between operations. */
public class Scope implements Serializable
  {
  /** Enum Kind */
  static public enum Kind
    {
      TAP, EACH, EVERY, GROUP, SPLICE
    }

  /** Field name */
  private String name;
  /** Field kind */
  private Kind kind;
  /** Field remainderFields */
  private Fields remainderFields;
  /** Field argumentSelector */
  private Fields argumentFields;
  /** Field declaredFields */
  private Fields declaredFields; // fields declared by the operation
  /** Field isGroupBy */
  private boolean isGroupBy;
  /** Field isMerge */
  private boolean isMerge;
  /** Field groupingSelectors */
  private Map<String, Fields> groupingSelectors;
  /** Field sortingSelectors */
  private Map<String, Fields> sortingSelectors;

  /** Field outGroupingSelector */
  private Fields outGroupingSelector;
  /** Field outGroupingFields */
  private Fields outGroupingFields; // all key fields

  /** Field outValuesSelector */
  private Fields outValuesSelector;
  /** Field outValuesFields */
  private Fields outValuesFields; // all value fields, includes keys

  /** Field argumentsEntry */
  private transient TupleEntry argumentsEntry; // caches entry
  /** Field declaredEntry */
  private transient TupleEntry declaredEntry; // caches entry

  /** Default constructor. */
  public Scope()
    {
    }

  /**
   * Copy constructor
   *
   * @param scope of type Scope
   */
  public Scope( Scope scope )
    {
    this.name = scope.getName();
    copyFields( scope );
    }

  /**
   * Tap constructor
   *
   * @param outFields of type Fields
   */
  public Scope( Fields outFields )
    {
    this.kind = Kind.TAP;

    if( outFields == null )
      throw new IllegalArgumentException( "fields may not be null" );

    this.outGroupingFields = outFields;
    this.outValuesFields = outFields;
    }

  /**
   * Constructor Scope creates a new Scope instance. Used by classes Each and Every.
   *
   * @param name              of type String
   * @param kind              of type Kind
   * @param remainderFields   of type Fields
   * @param argumentFields    of type Fields
   * @param declaredFields    of type Fields
   * @param outGroupingFields of type Fields
   * @param outValuesFields   of type Fields
   */
  public Scope( String name, Kind kind, Fields remainderFields, Fields argumentFields, Fields declaredFields, Fields outGroupingFields, Fields outValuesFields )
    {
    this.name = name;
    this.kind = kind;
    this.remainderFields = remainderFields;
    this.argumentFields = argumentFields;
    this.declaredFields = declaredFields;

    if( outGroupingFields == null )
      throw new IllegalArgumentException( "grouping may not be null" );

    if( outValuesFields == null )
      throw new IllegalArgumentException( "values may not be null" );

    if( kind == Kind.EACH )
      {
      this.outGroupingFields = Fields.asDeclaration( outGroupingFields );
      this.outValuesSelector = outValuesFields;
      this.outValuesFields = Fields.asDeclaration( outValuesFields );
      }
    else if( kind == Kind.EVERY )
      {
      this.outGroupingSelector = outGroupingFields;
      this.outGroupingFields = Fields.asDeclaration( outGroupingFields );
      this.outValuesFields = outValuesFields;
      }
    else
      {
      throw new IllegalArgumentException( "may not use the constructor for kind: " + kind );
      }
    }

  /**
   * Constructor Scope creates a new Scope instance. Used by the Group class.
   *
   * @param name              of type String
   * @param declaredFields    of type Fields
   * @param outGroupingFields of type Fields
   * @param groupingSelectors of type Map<String, Fields>
   * @param sortingSelectors  of type Fields
   * @param outValuesFields   of type Fields
   * @param isGroupBy         of type boolean
   */
  public Scope( String name, Fields declaredFields, Fields outGroupingFields, Map<String, Fields> groupingSelectors, Map<String, Fields> sortingSelectors, Fields outValuesFields, boolean isGroupBy )
    {
    this.name = name;
    this.kind = Kind.GROUP;
    this.isGroupBy = isGroupBy;

    if( groupingSelectors == null )
      throw new IllegalArgumentException( "grouping may not be null" );

    if( outValuesFields == null )
      throw new IllegalArgumentException( "values may not be null" );

    this.declaredFields = declaredFields;
    this.outGroupingFields = outGroupingFields;
    this.groupingSelectors = groupingSelectors;
    this.sortingSelectors = sortingSelectors; // null ok
    this.outValuesFields = outValuesFields;
    }

  public Scope( String name, Fields declaredFields, boolean isMerge )
    {
    this.name = name;
    this.kind = Kind.SPLICE;
    this.isMerge = isMerge;

    this.declaredFields = declaredFields;
    this.outValuesFields = declaredFields;
    }

  /**
   * Constructor Scope creates a new Scope instance.
   *
   * @param name of type String
   */
  public Scope( String name )
    {
    this.name = name;
    }

  /**
   * Method isGroup returns true if this Scope object represents a Group.
   *
   * @return the group (type boolean) of this Scope object.
   */
  public boolean isGroup()
    {
    return kind == Kind.GROUP;
    }

  /**
   * Method isEach returns true if this Scope object represents an Each.
   *
   * @return the each (type boolean) of this Scope object.
   */
  public boolean isEach()
    {
    return kind == Kind.EACH;
    }

  /**
   * Method isEvery returns true if this Scope object represents an Every.
   *
   * @return the every (type boolean) of this Scope object.
   */
  public boolean isEvery()
    {
    return kind == Kind.EVERY;
    }

  /**
   * Method isTap returns true if this Scope object represents a Tap.
   *
   * @return the tap (type boolean) of this Scope object.
   */
  public boolean isTap()
    {
    return kind == Kind.TAP;
    }

  /**
   * Method getName returns the name of this Scope object.
   *
   * @return the name (type String) of this Scope object.
   */
  public String getName()
    {
    return name;
    }

  /**
   * Method setName sets the name of this Scope object.
   *
   * @param name the name of this Scope object.
   */
  public void setName( String name )
    {
    this.name = name;
    }

  /**
   * Method getRemainderFields returns the remainderFields of this Scope object.
   *
   * @return the remainderFields (type Fields) of this Scope object.
   */
  public Fields getRemainderFields()
    {
    return remainderFields;
    }

  /**
   * Method getArgumentSelector returns the argumentSelector of this Scope object.
   *
   * @return the argumentSelector (type Fields) of this Scope object.
   */
  public Fields getArgumentsSelector()
    {
    return argumentFields;
    }

  /**
   * Method getArguments returns the arguments of this Scope object.
   *
   * @return the arguments (type Fields) of this Scope object.
   */
  public Fields getArgumentsDeclarator()
    {
    return Fields.asDeclaration( argumentFields );
    }

  /**
   * Method getArgumentsEntry returns the argumentsEntry of this Scope object.
   *
   * @return the argumentsEntry (type TupleEntry) of this Scope object.
   */
  public TupleEntry getArgumentsEntry()
    {
    if( argumentsEntry != null )
      return argumentsEntry;

    argumentsEntry = new TupleEntry( getArgumentsDeclarator(), true );

    return argumentsEntry;
    }

  /**
   * Method getArgumentsEntry returns a cached {@link TupleEntry} for the declared arguments of this scope.
   *
   * @param input of type TupleEntry
   * @return TupleEntry
   */
  public TupleEntry getArgumentsEntry( TupleEntry input )
    {
    TupleEntry entry = getArgumentsEntry();

    entry.setTuple( input.selectTuple( getArgumentsSelector() ) );

    return entry;
    }

  /**
   * Method getDeclaredFields returns the declaredFields of this Scope object.
   *
   * @return the declaredFields (type Fields) of this Scope object.
   */
  public Fields getDeclaredFields()
    {
    return declaredFields;
    }

  /**
   * Method getDeclaredEntry returns the declaredEntry of this Scope object.
   *
   * @return the declaredEntry (type TupleEntry) of this Scope object.
   */
  public TupleEntry getDeclaredEntry()
    {
    if( declaredEntry != null )
      return declaredEntry;

    declaredEntry = new TupleEntry( getDeclaredFields() );

    return declaredEntry;
    }

  /**
   * Method getGroupingSelectors returns the groupingSelectors of this Scope object.
   *
   * @return the groupingSelectors (type Map<String, Fields>) of this Scope object.
   */
  public Map<String, Fields> getGroupingSelectors()
    {
    return groupingSelectors;
    }

  /**
   * Method getSortingSelectors returns the sortingSelectors of this Scope object.
   *
   * @return the sortingSelectors (type Map<String, Fields>) of this Scope object.
   */
  public Map<String, Fields> getSortingSelectors()
    {
    return sortingSelectors;
    }

  /**
   * Method getOutGroupingSelector returns the outGroupingSelector of this Scope object.
   *
   * @return the outGroupingSelector (type Fields) of this Scope object.
   */
  public Fields getOutGroupingSelector()
    {
    return outGroupingSelector;
    }

  /**
   * Method getOutGroupingFields returns the outGroupingFields of this Scope object.
   *
   * @return the outGroupingFields (type Fields) of this Scope object.
   */
  public Fields getOutGroupingFields()
    {
    if( kind != Kind.GROUP )
      return outGroupingFields;

    Fields first = groupingSelectors.values().iterator().next();

    if( groupingSelectors.size() == 1 || isGroup() && isGroupBy )
      return first;

    // if given by user
    if( outGroupingFields != null )
      return outGroupingFields;

    // if all have the same names, then use for grouping
    Set<Fields> set = new HashSet<Fields>( groupingSelectors.values() );

    if( set.size() == 1 )
      return first;

    return Fields.size( first.size() );
    }

  /**
   * Method getOutValuesSelector returns the outValuesSelector of this Scope object.
   *
   * @return the outValuesSelector (type Fields) of this Scope object.
   */
  public Fields getOutValuesSelector()
    {
    return outValuesSelector;
    }

  /**
   * Method getOutValuesFields returns the outValuesFields of this Scope object.
   *
   * @return the outValuesFields (type Fields) of this Scope object.
   */
  public Fields getOutValuesFields()
    {
    return outValuesFields;
    }

  /**
   * Method copyFields copies the given Scope instance fields to this instance.
   *
   * @param scope of type Scope
   */
  public void copyFields( Scope scope )
    {
    this.kind = scope.kind;
    this.isGroupBy = scope.isGroupBy;
    this.remainderFields = scope.remainderFields;
    this.argumentFields = scope.argumentFields;
    this.declaredFields = scope.declaredFields;
    this.groupingSelectors = scope.groupingSelectors;
    this.sortingSelectors = scope.sortingSelectors;
    this.outGroupingSelector = scope.outGroupingSelector;
    this.outGroupingFields = scope.outGroupingFields;
    this.outValuesSelector = scope.outValuesSelector;
    this.outValuesFields = scope.outValuesFields;
    }

  @Override
  public String toString()
    {
    if( getOutValuesFields() == null )
      return ""; // endpipes

    StringBuffer buffer = new StringBuffer();

    if( groupingSelectors != null && !groupingSelectors.isEmpty() )
      {
      for( String name : groupingSelectors.keySet() )
        {
        if( buffer.length() != 0 )
          buffer.append( "," );
        buffer.append( name ).append( groupingSelectors.get( name ).printVerbose() );
        }

      buffer.append( "\n" );
      }

    if( outGroupingFields != null )
      buffer.append( getOutGroupingFields().printVerbose() ).append( "\n" );

    buffer.append( getOutValuesFields().printVerbose() );

    return buffer.toString();
    }
  }
