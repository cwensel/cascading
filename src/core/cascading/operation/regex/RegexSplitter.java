/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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
import java.util.regex.Pattern;

import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/** Class RegexSplitter will split an incoming argument value by the given regex delimiter patternString. */
public class RegexSplitter extends RegexOperation<Pattern> implements Function<Pattern>
  {
  /**
   * Constructor RegexSplitter creates a new RegexSplitter instance.
   *
   * @param patternString of type String
   */
  @ConstructorProperties({"patternString"})
  public RegexSplitter( String patternString )
    {
    super( 1, patternString );
    }

  /**
   * Constructor RegexOperation creates a new RegexOperation instance, where the delimiter is the tab character.
   *
   * @param fieldDeclaration of type Fields
   */
  @ConstructorProperties({"fieldDeclaration"})
  public RegexSplitter( Fields fieldDeclaration )
    {
    super( 1, fieldDeclaration, "\t" );
    }

  /**
   * Constructor RegexSplitter creates a new RegexSplitter instance.
   *
   * @param fieldDeclaration of type Fields
   * @param patternString    of type String
   */
  @ConstructorProperties({"fieldDeclaration", "patternString"})
  public RegexSplitter( Fields fieldDeclaration, String patternString )
    {
    super( 1, fieldDeclaration, patternString );
    }

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall<Pattern> operationCall )
    {
    operationCall.setContext( getPattern() );
    }

  /** @see Function#operate(cascading.flow.FlowProcess, cascading.operation.FunctionCall) */
  public void operate( FlowProcess flowProcess, FunctionCall<Pattern> functionCall )
    {
    String value = functionCall.getArguments().getString( 0 );

    if( value == null )
      value = "";

    Tuple output = new Tuple();

    // TODO: optimize this
    int length = fieldDeclaration.isUnknown() ? -1 : fieldDeclaration.size();
    String[] split = functionCall.getContext().split( value, length );

    for( int i = 0; i < split.length; i++ )
      output.add( split[ i ] );

    functionCall.getOutputCollector().add( output );
    }
  }
