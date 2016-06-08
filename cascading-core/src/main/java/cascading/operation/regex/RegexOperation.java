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

package cascading.operation.regex;

import java.beans.ConstructorProperties;
import java.util.regex.Pattern;

import cascading.management.annotation.Property;
import cascading.management.annotation.PropertyDescription;
import cascading.management.annotation.Visibility;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;

/** Class RegexOperation is the base class for all regex Operations. */
public class RegexOperation<C> extends BaseOperation<C>
  {
  /** Field patternString */
  protected String patternString = ".*";

  /** Constructor RegexOperation creates a new RegexOperation instance. */
  public RegexOperation()
    {
    }

  /**
   * Constructor RegexOperation creates a new RegexOperation instance.
   *
   * @param numArgs of type int
   */
  @ConstructorProperties({"numArgs"})
  public RegexOperation( int numArgs )
    {
    super( numArgs );
    }

  /**
   * Constructor RegexOperation creates a new RegexOperation instance.
   *
   * @param fieldDeclaration of type Fields
   */
  @ConstructorProperties({"fieldDeclaration"})
  public RegexOperation( Fields fieldDeclaration )
    {
    super( fieldDeclaration );
    }

  /**
   * Constructor RegexOperation creates a new RegexOperation instance.
   *
   * @param numArgs       of type int
   * @param patternString of type String
   */
  @ConstructorProperties({"numArgs", "patternString"})
  public RegexOperation( int numArgs, String patternString )
    {
    super( numArgs );
    this.patternString = patternString;
    }

  /**
   * Constructor RegexOperation creates a new RegexOperation instance.
   *
   * @param patternString of type String
   */
  @ConstructorProperties({"patternString"})
  public RegexOperation( String patternString )
    {
    this.patternString = patternString;
    }

  /**
   * Constructor RegexOperation creates a new RegexOperation instance.
   *
   * @param numArgs          of type int
   * @param fieldDeclaration of type Fields
   */
  @ConstructorProperties({"numArgs", "fieldDeclaration"})
  public RegexOperation( int numArgs, Fields fieldDeclaration )
    {
    super( numArgs, fieldDeclaration );
    }

  /**
   * Constructor RegexOperation creates a new RegexOperation instance.
   *
   * @param numArgs          of type int
   * @param fieldDeclaration of type Fields
   * @param patternString    of type String
   */
  @ConstructorProperties({"numArgs", "fieldDeclaration", "patternString"})
  public RegexOperation( int numArgs, Fields fieldDeclaration, String patternString )
    {
    super( numArgs, fieldDeclaration );
    this.patternString = patternString;
    }

  /**
   * Method getPatternString returns the patternString of this RegexOperation object.
   *
   * @return the patternString (type String) of this RegexOperation object.
   */
  @Property(name = "patternString", visibility = Visibility.PRIVATE)
  @PropertyDescription("The regular expression pattern string.")
  public final String getPatternString()
    {
    return patternString;
    }

  /**
   * Method getPattern returns the pattern of this RegexOperation object.
   *
   * @return the pattern (type Pattern) of this RegexOperation object.
   */
  protected Pattern getPattern()
    {
    return Pattern.compile( getPatternString() );
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof RegexOperation ) )
      return false;
    if( !super.equals( object ) )
      return false;

    RegexOperation that = (RegexOperation) object;

    if( patternString != null ? !patternString.equals( that.patternString ) : that.patternString != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( patternString != null ? patternString.hashCode() : 0 );
    return result;
    }
  }
