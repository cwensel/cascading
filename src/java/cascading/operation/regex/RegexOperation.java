/*
 * Copyright (c) 2007-2008 Chris K Wensel. All Rights Reserved.
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

package cascading.operation.regex;

import java.util.regex.Pattern;

import cascading.operation.Operation;
import cascading.tuple.Fields;

/** Class RegexOperation is the base class for all regex Operations. */
public class RegexOperation extends Operation
  {
  /** Field patternString */
  protected String patternString = ".*";

  /** Field pattern */
  private transient Pattern pattern;

  /** Constructor RegexOperation creates a new RegexOperation instance. */
  public RegexOperation()
    {
    }

  /**
   * Constructor RegexOperation creates a new RegexOperation instance.
   *
   * @param numArgs of type int
   */
  public RegexOperation( int numArgs )
    {
    super( numArgs );
    }

  /**
   * Constructor RegexOperation creates a new RegexOperation instance.
   *
   * @param fieldDeclaration of type Fields
   */
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
  public RegexOperation( int numArgs, String patternString )
    {
    super( numArgs );
    this.patternString = patternString;
    }

  /**
   * Constructor RegexOperation creates a new RegexOperation instance.
   *
   * @param numArgs          of type int
   * @param fieldDeclaration of type Fields
   */
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
  public RegexOperation( int numArgs, Fields fieldDeclaration, String patternString )
    {
    super( numArgs, fieldDeclaration );
    this.patternString = patternString;
    }

  /**
   * Method getPattern returns the pattern of this RegexOperation object.
   *
   * @return the pattern (type Pattern) of this RegexOperation object.
   */
  protected Pattern getPattern()
    {
    if( pattern != null )
      return pattern;

    pattern = Pattern.compile( patternString );

    return pattern;
    }
  }
