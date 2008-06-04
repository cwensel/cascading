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

package cascading.flow;

import java.io.Serializable;
import java.util.Map;

import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/** Class Scope ... */
public class Scope implements Serializable
  {
  static public enum Kind
    {
      TAP, EACH, EVERY, GROUP
    }

  /** Field name */
  private String name;
  /** Field kind */
  private Kind kind;
  /** Field argumentSelector */
  private Fields argumentSelector;
  /** Field declaredFields */
  private Fields declaredFields; // fields declared by the operation
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
   * @param argumentSelector  of type Fields
   * @param declaredFields    of type Fields
   * @param outGroupingFields of type Fields
   * @param outValuesFields   of type Fields
   */
  public Scope( String name, Kind kind, Fields argumentSelector, Fields declaredFields, Fields outGroupingFields, Fields outValuesFields )
    {
    this.name = name;
    this.kind = kind;
    this.argumentSelector = argumentSelector;
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
   * @param groupingSelectors of type Map<String, Fields>
   * @param outValuesFields   of type Fields
   */
  public Scope( String name, Fields declaredFields, Map<String, Fields> groupingSelectors, Map<String, Fields> sortingSelectors, Fields outValuesFields )
    {
    this.name = name;
    this.kind = Kind.GROUP;

    if( groupingSelectors == null )
      throw new IllegalArgumentException( "grouping may not be null" );

    if( outValuesFields == null )
      throw new IllegalArgumentException( "values may not be null" );

    this.declaredFields = declaredFields;
    this.groupingSelectors = groupingSelectors;
    this.sortingSelectors = sortingSelectors; // null ok
    this.outValuesFields = outValuesFields;
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
   * Method getArgumentSelector returns the argumentSelector of this Scope object.
   *
   * @return the argumentSelector (type Fields) of this Scope object.
   */
  public Fields getArgumentSelector()
    {
    return argumentSelector;
    }

  /**
   * Method getArguments returns the arguments of this Scope object.
   *
   * @return the arguments (type Fields) of this Scope object.
   */
  public Fields getArguments()
    {
    return Fields.asDeclaration( argumentSelector );
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

    argumentsEntry = new TupleEntry( getArguments() );

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

    entry.setTuple( input.selectTuple( getArgumentSelector() ) );

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

    if( groupingSelectors.size() == 1 )
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
    this.argumentSelector = scope.argumentSelector;
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
        buffer.append( name ).append( groupingSelectors.get( name ).print() );
        }

      buffer.append( "\n" );
      }

    if( outGroupingFields != null )
      buffer.append( getOutGroupingFields().print() ).append( "\n" );

    buffer.append( getOutValuesFields().print() );

    return buffer.toString();
    }

  }
