/*
 * Copyright (c) 2007-2010 Concurrent, Inc. All Rights Reserved.
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

/** Class RegexGenerator will emit a new Tuple for every matched regex group. */
public class RegexGenerator extends RegexOperation<Matcher> implements Function<Matcher>
  {
  /**
   * Constructor RegexGenerator creates a new RegexGenerator instance.
   *
   * @param patternString of type String
   */
  @ConstructorProperties({"patternString"})
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
  @ConstructorProperties({"fieldDeclaration", "patternString"})
  public RegexGenerator( Fields fieldDeclaration, String patternString )
    {
    super( 1, fieldDeclaration, patternString );

    if( fieldDeclaration.size() != 1 )
      throw new IllegalArgumentException( "fieldDeclaration may only declare one field, was " + fieldDeclaration.print() );
    }

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall<Matcher> operationCall )
    {
    operationCall.setContext( getPattern().matcher( "" ) );
    }

  /** @see Function#operate(cascading.flow.FlowProcess,cascading.operation.FunctionCall) */
  public void operate( FlowProcess flowProcess, FunctionCall<Matcher> functionCall )
    {
    String value = functionCall.getArguments().getString( 0 );

    if( value == null )
      value = "";

    Matcher matcher = functionCall.getContext().reset( value );

    while( matcher.find() )
      functionCall.getOutputCollector().add( new Tuple( matcher.group() ) );
    }
  }
