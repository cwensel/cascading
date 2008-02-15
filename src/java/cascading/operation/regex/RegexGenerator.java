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
import cascading.operation.generator.Generator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleCollector;
import cascading.tuple.TupleEntry;

/** Class RegexGenerator will emit a new Tuple for every matched regex group. */
public class RegexGenerator extends RegexOperation implements Generator
  {
  /**
   * Constructor RegexGenerator creates a new RegexGenerator instance.
   *
   * @param patternString of type String
   */
  public RegexGenerator( String patternString )
    {
    super( 1, Fields.size( 1 ), patternString );
    }

  /**
   * Constructor RegexGenerator creates a new RegexGenerator instance.
   *
   * @param fieldDeclaration of type Fields
   * @param patternString    of type String
   */
  public RegexGenerator( Fields fieldDeclaration, String patternString )
    {
    super( 1, fieldDeclaration, patternString );
    }

  /** @see Function#operate(TupleEntry, TupleCollector) */
  public void operate( TupleEntry input, TupleCollector outputCollector )
    {
    String value = (String) input.get( 0 );

    Matcher matcher = getPattern().matcher( value );

    while( matcher.find() )
      outputCollector.add( new Tuple( matcher.group() ) );
    }
  }
