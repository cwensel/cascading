/*
 * Copyright (c) 2016-2019 Chris K Wensel. All Rights Reserved.
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
import cascading.operation.OperationException;
import heretical.pointer.path.NestedPointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class NestedRegexFilter is the base class for {@link Filter} implementations that want to filter a tuple stream
 * based on the values in a nested object tree.
 * <p>
 * {@link cascading.tuple.Tuple} instances are retained if any of the {@link Pattern} instances match.
 * <p>
 * By default a {@code null} value will throw an {@link OperationException} unless {@code failOnMissingNode} is true,
 * the {@code null} will be converted to an empty string before being passed to a pattern for matching.
 * <p>
 * Note that a wildcard or descent pointer can be used which may return multiple elements, each of which will be tested,
 * and any match will trigger the filter.
 * <p>
 * Use the {@link cascading.operation.filter.Not} filter to negate this filter.
 */
public class NestedRegexFilter<Node, Results> extends NestedBaseOperation<Node, Results, Matcher[]> implements Filter<Matcher[]>
  {
  private static final Logger LOG = LoggerFactory.getLogger( NestedRegexFilter.class );

  private static final String EMPTY = "";

  final NestedPointer<Node, Results> pointer;
  final List<Pattern> patterns;
  final boolean failOnMissingNode;

  public NestedRegexFilter( NestedCoercibleType<Node, Results> nestedCoercibleType, String pointer, List<Pattern> patterns, boolean failOnMissingNode )
    {
    super( nestedCoercibleType );
    this.pointer = getNestedPointerCompiler().nested( pointer );
    this.patterns = new ArrayList<>( patterns );
    this.failOnMissingNode = failOnMissingNode;
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
    Results results = pointer.allAt( node );

    if( size( results ) == 0 )
      {
      if( failOnMissingNode )
        throw new OperationException( "node missing from json node tree: " + pointer );

      for( Matcher matcher : filterCall.getContext() )
        {
        matcher.reset( EMPTY );

        boolean found = matcher.find();

        if( LOG.isDebugEnabled() )
          LOG.debug( "pointer: {}, pattern: {}, matches: {}, on empty string, no json node found with ", pointer, matcher.pattern().pattern(), found );

        if( found )
          return false;
        }

      return true;
      }

    Iterable<Node> iterable = iterable( results );

    for( Node result : iterable )
      {
      String value = getCoercibleType().coerce( result, String.class );

      if( value == null )
        value = EMPTY;

      for( Matcher matcher : filterCall.getContext() )
        {
        matcher.reset( value );

        boolean found = matcher.find();

        if( LOG.isDebugEnabled() )
          LOG.debug( "pointer: {}, pattern: {}, matches: {}, element: {}", pointer, matcher.pattern().pattern(), found, value );

        if( found )
          return false;
        }
      }

    return true;
    }
  }
