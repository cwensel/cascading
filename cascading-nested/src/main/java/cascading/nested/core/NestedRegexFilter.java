/*
 * Copyright (c) 2016-2017 Chris K Wensel. All Rights Reserved.
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

package cascading.nested.core;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cascading.flow.FlowProcess;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.OperationCall;
import heretical.pointer.path.Pointer;

/**
 * Class NestedRegexFilter is the base class for {@link Filter} implementations that want to filter a tuple stream
 * based on the values in a nested object tree.
 * <p>
 * {@link cascading.tuple.Tuple} instances are retained if any of the {@link Pattern} instances match.
 * <p>
 * Any {@code null} values will be converted to an empty string before being passed to a pattern for matching.
 */
public class NestedRegexFilter<Node, Results> extends NestedBaseOperation<Node, Results, Matcher[]> implements Filter<Matcher[]>
  {
  private static final String EMPTY = "";

  final Pointer<Node> pointer;
  final List<Pattern> patterns;

  public NestedRegexFilter( NestedCoercibleType<Node, Results> nestedCoercibleType, String pointer, List<Pattern> patterns )
    {
    super( nestedCoercibleType );
    this.pointer = getNestedPointerCompiler().compile( pointer );
    this.patterns = new ArrayList<>( patterns );
    }

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall<Matcher[]> operationCall )
    {
    Matcher[] matchers = new Matcher[ patterns.size() ];

    for( int i = 0; i < patterns.size(); i++ )
      matchers[ i ] = patterns.get( i ).matcher( "" );

    operationCall.setContext( matchers );
    }

  @Override
  public boolean isRemove( FlowProcess flowProcess, FilterCall<Matcher[]> filterCall )
    {
    Node node = (Node) filterCall.getArguments().getObject( 0, getCoercibleType() );
    Node result = pointer.at( node );

    String value = getCoercibleType().coerce( result, String.class );

    if( value == null )
      value = EMPTY;

    for( Matcher matcher : filterCall.getContext() )
      {
      matcher.reset( value );

      if( matcher.find() )
        return false;
      }

    return true;
    }
  }
