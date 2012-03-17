/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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

import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.util.Pair;

/**
 * Class RegexGenerator will emit a new Tuple for every split on the incoming argument value delimited by the given patternString.
 * <p/>
 * This could be used to break a document into single word tuples for later processing for a word count.
 */
public class RegexSplitGenerator extends RegexOperation<Pair<Pattern, Tuple>> implements Function<Pair<Pattern, Tuple>>
  {
  /**
   * Constructor RegexGenerator creates a new RegexGenerator instance.
   *
   * @param patternString of type String
   */
  @ConstructorProperties({"patternString"})
  public RegexSplitGenerator( String patternString )
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
  public RegexSplitGenerator( Fields fieldDeclaration, String patternString )
    {
    super( 1, fieldDeclaration, patternString );

    if( fieldDeclaration.size() != 1 )
      throw new IllegalArgumentException( "fieldDeclaration may only declare one field, was " + fieldDeclaration.print() );
    }

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall<Pair<Pattern, Tuple>> operationCall )
    {
    operationCall.setContext( new Pair<Pattern, Tuple>( getPattern(), Tuple.size( 1 ) ) );
    }

  @Override
  public void operate( FlowProcess flowProcess, FunctionCall<Pair<Pattern, Tuple>> functionCall )
    {
    String value = functionCall.getArguments().getString( 0 );

    if( value == null )
      value = "";

    String[] split = functionCall.getContext().getLhs().split( value );

    for( String string : split )
      {
      functionCall.getContext().getRhs().set( 0, string );
      functionCall.getOutputCollector().add( functionCall.getContext().getRhs() );
      }
    }
  }