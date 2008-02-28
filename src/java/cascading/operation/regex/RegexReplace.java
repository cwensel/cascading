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
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleCollector;
import cascading.tuple.TupleEntry;

/** Class RegexReplace is used to replace a matched regex with a replacement value. */
public class RegexReplace extends RegexOperation implements Function
  {
  /** Field replacement */
  private final String replacement;
  /** Field replaceAll */
  private boolean replaceAll = true;

  /**
   * Constructor RegexReplace creates a new RegexReplace instance,
   *
   * @param fieldDeclaration of type Fields
   * @param patternString    of type String
   * @param replacement      of type String
   * @param replaceAll       of type boolean
   */
  public RegexReplace( Fields fieldDeclaration, String patternString, String replacement, boolean replaceAll )
    {
    this( fieldDeclaration, patternString, replacement );
    this.replaceAll = replaceAll;
    }

  /**
   * Constructor RegexReplace creates a new RegexReplace instance.
   *
   * @param fieldDeclaration of type Fields
   * @param patternString    of type String
   * @param replacement      of type String
   */
  public RegexReplace( Fields fieldDeclaration, String patternString, String replacement )
    {
    super( 1, fieldDeclaration, patternString );
    this.replacement = replacement;
    }

  /** @see Function#operate(TupleEntry, TupleCollector) */
  public void operate( TupleEntry input, TupleCollector outputCollector )
    {
    // coerce to string
    String value = (String) input.getTuple().getString( 0 );

    // make safe
    if( value == null )
      value = "";

    Tuple output = new Tuple();

    // todo: reuse the matcher via the .reset() method. need to confirm only one thread will fire through this
    Matcher matcher = getPattern().matcher( value );

    if( replaceAll )
      output.add( matcher.replaceAll( replacement ) );
    else
      output.add( matcher.replaceFirst( replacement ) );

    outputCollector.add( output );
    }
  }
