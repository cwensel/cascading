/*
 * Copyright (c) 2007-20010 Concurrent, Inc. All Rights Reserved.
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

import java.beans.ConstructorProperties;
import java.util.regex.Matcher;

import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/** Class RegexReplace is used to replace a matched regex with a replacement value. */
public class RegexReplace extends RegexOperation<Matcher> implements Function<Matcher>
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
  @ConstructorProperties({"fieldDeclaration", "patternString", "replacement", "replaceAll"})
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
  @ConstructorProperties({"fieldDeclaration", "patternString", "replacement"})
  public RegexReplace( Fields fieldDeclaration, String patternString, String replacement )
    {
    super( 1, fieldDeclaration, patternString );
    this.replacement = replacement;
    }

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall<Matcher> operationCall )
    {
    operationCall.setContext( getPattern().matcher( "" ) );
    }

  /** @see Function#operate(cascading.flow.FlowProcess,cascading.operation.FunctionCall) */
  public void operate( FlowProcess flowProcess, FunctionCall<Matcher> functionCall )
    {
    // coerce to string
    String value = functionCall.getArguments().getString( 0 );

    // make safe
    if( value == null )
      value = "";

    Tuple output = new Tuple();

    Matcher matcher = functionCall.getContext().reset( value );

    if( replaceAll )
      output.add( matcher.replaceAll( replacement ) );
    else
      output.add( matcher.replaceFirst( replacement ) );

    functionCall.getOutputCollector().add( output );
    }
  }
