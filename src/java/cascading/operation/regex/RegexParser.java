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

package cascading.operation.regex;

import java.util.regex.Matcher;

import cascading.operation.Function;
import cascading.operation.OperationException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryListIterator;

/** Class RegexParser ... */
public class RegexParser extends RegexOperation implements Function
  {
  /** Field groups */
  private int[] groups = new int[]{0};

  /**
   * Constructor RegexParser creates a new RegexParser instance, where the argument Tuple value is matched and returned
   * in a new field.
   *
   * @param patternString of type String
   */
  public RegexParser( String patternString )
    {
    super( 1, Fields.size( 1 ), patternString );
    }

  /**
   * Constructor RegexParser creates a new RegexParser instance, where the patternString is a simple regular expression
   * whose match value is stored in the new field named by the given fieldDeclaration.
   *
   * @param fieldDeclaration of type Fields
   * @param patternString    of type String
   */
  public RegexParser( Fields fieldDeclaration, String patternString )
    {
    super( 1, fieldDeclaration, patternString );
    }

  /**
   * Constructor RegexParser creates a new RegexParser instance, where the patternString is a regular expression
   * with match groups and whose groups designated by groupPos are stored in the appropriate number of new fields.
   *
   * @param patternString of type String
   * @param groupPos      of type int[]
   */
  public RegexParser( String patternString, int[] groupPos )
    {
    super( 1, patternString );
    this.groups = groupPos;
    }

  /**
   * Constructor RegexParser creates a new RegexParser instance, where the patternString is a regular expression
   * with match groups and whose groups designated by groupPos are stored in the named fieldDeclarations.
   *
   * @param fieldDeclaration of type Fields
   * @param patternString    of type String
   * @param groups           of type int[]
   */
  public RegexParser( Fields fieldDeclaration, String patternString, int[] groups )
    {
    super( 1, fieldDeclaration, patternString );
    this.groups = groups;
    }

  /** @see Function#operate(TupleEntry, TupleEntryListIterator) */
  public void operate( TupleEntry input, TupleEntryListIterator outputCollector )
    {
    String value = (String) input.get( 0 );
    Tuple output = new Tuple();

    // todo: reuse the matcher via the .reset() method. need to confirm only one thread will fire through this
    Matcher matcher = getPattern().matcher( value );

    if( !matcher.find() )
      throw new OperationException( "could not match pattern: " + getPattern() + " with value: " + value );

    for( int pos : groups )
      output.add( matcher.group( pos ) );

    outputCollector.add( output );
    }
  }
