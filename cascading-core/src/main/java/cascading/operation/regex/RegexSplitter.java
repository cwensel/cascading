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

import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.util.Pair;

/**
 * Class RegexSplitter will split an incoming argument value by the given regex delimiter patternString.
 * <p/>
 * RegexSplitter only expects one field value. If more than one argument value is passed, only the
 * first is handled, the remainder are ignored.
 * <p/>
 * Note a {@code null} valued argument passed to the parser will be converted to an empty string ({@code ""}) before
 * the regex is applied.
 * <p/>
 * Any Object value will be coerced to a String type if type information is provided. See the
 * {@link cascading.tuple.type.CoercibleType} interface to control how custom Object types are converted to String
 * values.
 */
public class RegexSplitter extends RegexOperation<Pair<Pattern, Tuple>> implements Function<Pair<Pattern, Tuple>>
  {
  private final int length;

  /**
   * Constructor RegexSplitter creates a new RegexSplitter instance.
   *
   * @param patternString of type String
   */
  @ConstructorProperties({"patternString"})
  public RegexSplitter( String patternString )
    {
    super( 1, patternString );
    length = fieldDeclaration.isUnknown() ? -1 : fieldDeclaration.size();
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
    length = fieldDeclaration.isUnknown() ? -1 : fieldDeclaration.size();
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
    length = fieldDeclaration.isUnknown() ? -1 : fieldDeclaration.size();
    }

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall<Pair<Pattern, Tuple>> operationCall )
    {
    operationCall.setContext( new Pair<Pattern, Tuple>( getPattern(), new Tuple() ) );
    }

  @Override
  public void operate( FlowProcess flowProcess, FunctionCall<Pair<Pattern, Tuple>> functionCall )
    {
    String value = functionCall.getArguments().getString( 0 );

    if( value == null )
      value = "";

    Tuple output = functionCall.getContext().getRhs();

    output.clear();

    String[] split = functionCall.getContext().getLhs().split( value, length );

    for( int i = 0; i < split.length; i++ )
      output.add( split[ i ] );

    functionCall.getOutputCollector().add( output );
    }
  }
