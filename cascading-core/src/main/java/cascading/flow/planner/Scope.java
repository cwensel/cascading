/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.planner;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import cascading.tuple.Fields;

import static cascading.tuple.Fields.asDeclaration;

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
  /** Field incomingPassThroughFields */
  private Fields incomingPassThroughFields;
  /** Field remainderPassThroughFields */
  private Fields remainderPassThroughFields;

  /** Field argumentSelector */
  private Fields operationArgumentFields;
  /** Field declaredFields */
  private Fields operationDeclaredFields; // fields declared by the operation
  /** Field isGroupBy */
  private boolean isGroupBy;

  /** Field groupingSelectors */
  private Map<String, Fields> keySelectors;
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
   * @param name                      of type String
   * @param kind                      of type Kind
   * @param incomingPassThroughFields //   * @param remainderPassThroughFields   of type Fields
   * @param operationArgumentFields   of type Fields
   * @param operationDeclaredFields   of type Fields
   * @param outGroupingFields         of type Fields
   * @param outValuesFields           of type Fields
   */
  public Scope( String name, Kind kind, Fields incomingPassThroughFields, Fields remainderPassThroughFields, Fields operationArgumentFields, Fields operationDeclaredFields, Fields outGroupingFields, Fields outValuesFields )
    {
    this.name = name;
    this.kind = kind;
    this.incomingPassThroughFields = incomingPassThroughFields;
    this.remainderPassThroughFields = remainderPassThroughFields;
    this.operationArgumentFields = operationArgumentFields;
    this.operationDeclaredFields = operationDeclaredFields;

    if( outGroupingFields == null )
      throw new IllegalArgumentException( "grouping may not be null" );

    if( outValuesFields == null )
      throw new IllegalArgumentException( "values may not be null" );

    if( kind == Kind.EACH )
      {
      this.outGroupingSelector = outGroupingFields;
      this.outGroupingFields = asDeclaration( outGroupingFields );
      this.outValuesSelector = outValuesFields;
      this.outValuesFields = asDeclaration( outValuesFields );
      }
    else if( kind == Kind.EVERY )
      {
      this.outGroupingSelector = outGroupingFields;
      this.outGroupingFields = asDeclaration( outGroupingFields );
      this.outValuesSelector = outValuesFields;
      this.outValuesFields = asDeclaration( outValuesFields );
      }
    else
      {
      throw new IllegalArgumentException( "may not use the constructor for kind: " + kind );
      }
    }

  /**
   * Constructor Scope creates a new Scope instance. Used by the Group class.
   *
   * @param name                    of type String
   * @param operationDeclaredFields of type Fields
   * @param outGroupingFields       of type Fields
   * @param keySelectors            of type Map<String, Fields>
   * @param sortingSelectors        of type Fields
   * @param outValuesFields         of type Fields
   * @param isGroupBy               of type boolean
   */
  public Scope( String name, Fields operationDeclaredFields, Fields outGroupingFields, Map<String, Fields> keySelectors, Map<String, Fields> sortingSelectors, Fields outValuesFields, boolean isGroupBy )
    {
    this.name = name;
    this.kind = Kind.GROUP;
    this.isGroupBy = isGroupBy;

    if( keySelectors == null )
      throw new IllegalArgumentException( "grouping may not be null" );

    if( outValuesFields == null )
      throw new IllegalArgumentException( "values may not be null" );

    this.operationDeclaredFields = operationDeclaredFields;
    this.outGroupingFields = asDeclaration( outGroupingFields );
    this.keySelectors = keySelectors;
    this.sortingSelectors = sortingSelectors; // null ok
    this.outValuesFields = asDeclaration( outValuesFields );
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
  public Fields getRemainderPassThroughFields()
    {
    return remainderPassThroughFields;
    }

  /**
   * Method getArgumentSelector returns the argumentSelector of this Scope object.
   *
   * @return the argumentSelector (type Fields) of this Scope object.
   */
  public Fields getArgumentsSelector()
    {
    return operationArgumentFields;
    }

  /**
   * Method getArguments returns the arguments of this Scope object.
   *
   * @return the arguments (type Fields) of this Scope object.
   */
  public Fields getArgumentsDeclarator()
    {
    return asDeclaration( operationArgumentFields );
    }

  /**
   * Method getDeclaredFields returns the declaredFields of this Scope object.
   *
   * @return the declaredFields (type Fields) of this Scope object.
   */
  public Fields getOperationDeclaredFields()
    {
    return operationDeclaredFields;
    }

  /**
   * Method getGroupingSelectors returns the groupingSelectors of this Scope object.
   *
   * @return the groupingSelectors (type Map<String, Fields>) of this Scope object.
   */
  public Map<String, Fields> getKeySelectors()
    {
    return keySelectors;
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

  public Fields getIncomingTapFields()
    {
    if( isEvery() )
      return getOutGroupingFields();
    else
      return getOutValuesFields();
    }

  public Fields getIncomingFunctionArgumentFields()
    {
    if( isEvery() )
      return getOutGroupingFields();
    else
      return getOutValuesFields();
    }

  public Fields getIncomingFunctionPassThroughFields()
    {
    if( isEvery() )
      return getOutGroupingFields();
    else
      return getOutValuesFields();
    }

  public Fields getIncomingAggregatorArgumentFields()
    {
    if( isEach() || isTap() )
      throw new IllegalStateException( "Every cannot follow a Tap or an Each" );

    return getOutValuesFields();
    }

  public Fields getIncomingAggregatorPassThroughFields()
    {
    if( isEach() || isTap() )
      throw new IllegalStateException( "Every cannot follow a Tap or an Each" );

    return getOutGroupingFields();
    }

  public Fields getIncomingBufferArgumentFields()
    {
    if( isEach() || isTap() )
      throw new IllegalStateException( "Every cannot follow a Tap or an Each" );

    return getOutValuesFields();
    }

  public Fields getIncomingBufferPassThroughFields()
    {
    if( isEach() || isTap() )
      throw new IllegalStateException( "Every cannot follow a Tap or an Each" );

    return getOutValuesFields();
    }

  public Fields getIncomingSpliceFields()
    {
    if( isEvery() )
      return getOutGroupingFields();
    else
      return getOutValuesFields();
    }

  /**
   * Method getOutGroupingFields returns the outGroupingFields of this Scope object.
   *
   * @return the outGroupingFields (type Fields) of this Scope object.
   */
  public Fields getOutGroupingFields()
    {
    if( !isGroup() )
      return outGroupingFields;

    Fields first = keySelectors.values().iterator().next();

    // if more than one, this is a merge, so same key names are expected
    if( keySelectors.size() == 1 || isGroup() && isGroupBy )
      return first;

    // handling CoGroup only

    // if given by user as resultGroupFields
    if( outGroupingFields != null )
      return outGroupingFields;

    // todo throw an exception if we make it this far

    // if all have the same names, then use for grouping
    Set<Fields> set = new HashSet<Fields>( keySelectors.values() );

    if( set.size() == 1 )
      return first;

    return Fields.size( first.size() );
    }

  public Fields getOutGroupingValueFields()
    {
    return getOutValuesFields().subtract( getOutGroupingFields() );
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
    this.incomingPassThroughFields = scope.incomingPassThroughFields;
    this.remainderPassThroughFields = scope.remainderPassThroughFields;
    this.operationArgumentFields = scope.operationArgumentFields;
    this.operationDeclaredFields = scope.operationDeclaredFields;
    this.keySelectors = scope.keySelectors;
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

    if( keySelectors != null && !keySelectors.isEmpty() )
      {
      for( String name : keySelectors.keySet() )
        {
        if( buffer.length() != 0 )
          buffer.append( "," );
        buffer.append( name ).append( keySelectors.get( name ).printVerbose() );
        }

      buffer.append( "\n" );
      }

    if( outGroupingFields != null )
      buffer.append( getOutGroupingFields().printVerbose() ).append( "\n" );

    buffer.append( getOutValuesFields().printVerbose() );

    return buffer.toString();
    }
  }
