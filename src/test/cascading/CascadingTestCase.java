/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

package cascading;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.Buffer;
import cascading.operation.ConcreteCall;
import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleListCollector;
import junit.framework.TestCase;

/**
 * Class CascadingTestCase is the base class for all Cascading tests.
 * <p/>
 * It included a few helpful utility methods for testing Cascading applications.
 */
public class CascadingTestCase extends TestCase implements Serializable
  {

  public CascadingTestCase()
    {
    }

  public CascadingTestCase( String name )
    {
    super( name );
    }

  protected void validateLength( Flow flow, int length ) throws IOException
    {
    validateLength( flow, length, -1 );
    }

  protected void validateLength( Flow flow, int length, String name ) throws IOException
    {
    validateLength( flow, length, -1, null, name );
    }

  protected void validateLength( Flow flow, int length, int size ) throws IOException
    {
    validateLength( flow, length, size, null, null );
    }

  protected void validateLength( Flow flow, int length, int size, Pattern regex ) throws IOException
    {
    validateLength( flow, length, size, regex, null );
    }

  protected void validateLength( Flow flow, int length, Pattern regex, String name ) throws IOException
    {
    validateLength( flow, length, -1, regex, name );
    }

  protected void validateLength( Flow flow, int length, int size, Pattern regex, String name ) throws IOException
    {
    TupleEntryIterator iterator = name == null ? flow.openSink() : flow.openSink( name );
    validateLength( iterator, length, size, regex );
    }

  protected void validateLength( TupleEntryIterator iterator, int length )
    {
    validateLength( iterator, length, -1, null );
    }

  protected void validateLength( TupleEntryIterator iterator, int length, int size )
    {
    validateLength( iterator, length, size, null );
    }

  protected void validateLength( TupleEntryIterator iterator, int length, Pattern regex )
    {
    validateLength( iterator, length, -1, regex );
    }

  protected void validateLength( TupleEntryIterator iterator, int length, int size, Pattern regex )
    {
    int count = 0;

    while( iterator.hasNext() )
      {
      TupleEntry tupleEntry = iterator.next();

      if( size != -1 )
        assertEquals( "wrong number of elements", size, tupleEntry.size() );

      if( regex != null )
        assertTrue( "regex: " + regex + " does not match: " + tupleEntry.getTuple().toString(), regex.matcher( tupleEntry.getTuple().toString() ).matches() );

      count++;
      }

    try
      {
      iterator.close();
      }
    catch( IOException exception )
      {
      throw new RuntimeException( exception );
      }

    assertEquals( "wrong number of lines", length, count );
    }

  protected TupleListCollector invokeFunction( Function function, Tuple arguments, Fields resultFields )
    {
    return invokeFunction( function, new TupleEntry( arguments ), resultFields );
    }

  protected TupleListCollector invokeFunction( Function function, TupleEntry arguments, Fields resultFields )
    {
    ConcreteCall operationCall = new ConcreteCall();
    TupleListCollector collector = new TupleListCollector( resultFields );

    operationCall.setArguments( arguments );
    operationCall.setOutputCollector( collector );

    function.prepare( FlowProcess.NULL, operationCall );
    function.operate( FlowProcess.NULL, operationCall );
    function.cleanup( FlowProcess.NULL, operationCall );

    return collector;
    }

  protected TupleListCollector invokeFunction( Function function, Tuple[] argumentsArray, Fields resultFields )
    {
    TupleEntry[] entries = makeArgumentsArray( argumentsArray );

    return invokeFunction( function, entries, resultFields );
    }

  protected TupleListCollector invokeFunction( Function function, TupleEntry[] argumentsArray, Fields resultFields )
    {
    ConcreteCall operationCall = new ConcreteCall();
    TupleListCollector collector = new TupleListCollector( resultFields );

    function.prepare( FlowProcess.NULL, operationCall );
    operationCall.setOutputCollector( collector );

    for( TupleEntry arguments : argumentsArray )
      {
      operationCall.setArguments( arguments );
      function.operate( FlowProcess.NULL, operationCall );
      }

    function.cleanup( FlowProcess.NULL, operationCall );

    return collector;
    }

  protected boolean invokeFilter( Filter filter, Tuple arguments )
    {
    return invokeFilter( filter, new TupleEntry( arguments ) );
    }

  protected boolean invokeFilter( Filter filter, TupleEntry arguments )
    {
    ConcreteCall operationCall = new ConcreteCall();

    operationCall.setArguments( arguments );

    filter.prepare( FlowProcess.NULL, operationCall );

    boolean isRemove = filter.isRemove( FlowProcess.NULL, operationCall );

    filter.cleanup( FlowProcess.NULL, operationCall );

    return isRemove;
    }

  protected boolean[] invokeFilter( Filter filter, Tuple[] argumentsArray )
    {
    TupleEntry[] entries = makeArgumentsArray( argumentsArray );

    return invokeFilter( filter, entries );
    }

  protected boolean[] invokeFilter( Filter filter, TupleEntry[] argumentsArray )
    {
    ConcreteCall operationCall = new ConcreteCall();

    filter.prepare( FlowProcess.NULL, operationCall );

    boolean[] results = new boolean[ argumentsArray.length ];

    for( int i = 0; i < argumentsArray.length; i++ )
      {
      operationCall.setArguments( argumentsArray[ i ] );

      results[ i ] = filter.isRemove( FlowProcess.NULL, operationCall );
      }

    filter.cleanup( FlowProcess.NULL, operationCall );

    return results;
    }

  protected TupleListCollector invokeAggregator( Aggregator aggregator, Tuple[] argumentsArray, Fields resultFields )
    {
    TupleEntry[] entries = makeArgumentsArray( argumentsArray );

    return invokeAggregator( aggregator, entries, resultFields );
    }

  protected TupleListCollector invokeAggregator( Aggregator aggregator, TupleEntry[] argumentsArray, Fields resultFields )
    {
    return invokeAggregator( aggregator, null, argumentsArray, resultFields );
    }

  protected TupleListCollector invokeAggregator( Aggregator aggregator, TupleEntry group, TupleEntry[] argumentsArray, Fields resultFields )
    {
    ConcreteCall operationCall = new ConcreteCall();

    operationCall.setGroup( group );

    aggregator.prepare( FlowProcess.NULL, operationCall );

    aggregator.start( FlowProcess.NULL, operationCall );

    for( TupleEntry arguments : argumentsArray )
      {
      operationCall.setArguments( arguments );
      aggregator.aggregate( FlowProcess.NULL, operationCall );
      }

    TupleListCollector collector = new TupleListCollector( resultFields );
    operationCall.setOutputCollector( collector );

    aggregator.complete( FlowProcess.NULL, operationCall );

    aggregator.cleanup( null, operationCall );

    return collector;
    }

  protected TupleListCollector invokeBuffer( Buffer buffer, Tuple[] argumentsArray, Fields resultFields )
    {
    TupleEntry[] entries = makeArgumentsArray( argumentsArray );

    return invokeBuffer( buffer, entries, resultFields );
    }

  protected TupleListCollector invokeBuffer( Buffer buffer, TupleEntry[] argumentsArray, Fields resultFields )
    {
    return invokeBuffer( buffer, null, argumentsArray, resultFields );
    }

  protected TupleListCollector invokeBuffer( Buffer buffer, TupleEntry group, TupleEntry[] argumentsArray, Fields resultFields )
    {
    ConcreteCall operationCall = new ConcreteCall();

    operationCall.setGroup( group );

    buffer.prepare( FlowProcess.NULL, operationCall );
    TupleListCollector collector = new TupleListCollector( resultFields );
    operationCall.setOutputCollector( collector );

    operationCall.setArgumentsIterator( Arrays.asList( argumentsArray ).iterator() );

    buffer.operate( FlowProcess.NULL, operationCall );

    buffer.cleanup( null, operationCall );

    return collector;
    }

  private TupleEntry[] makeArgumentsArray( Tuple[] argumentsArray )
    {
    TupleEntry[] entries = new TupleEntry[ argumentsArray.length ];

    for( int i = 0; i < argumentsArray.length; i++ )
      entries[ i ] = new TupleEntry( argumentsArray[ i ] );

    return entries;
    }

  protected List<Tuple> getSourceAsList( Flow flow ) throws IOException
    {
    TupleEntryIterator iterator = flow.openSource();

    List<Tuple> result = asCollection( iterator, new ArrayList<Tuple>() );

    iterator.close();

    return result;
    }

  protected List<Tuple> getSinkAsList( Flow flow ) throws IOException
    {
    TupleEntryIterator iterator = flow.openSink();

    List<Tuple> result = asCollection( iterator, new ArrayList<Tuple>() );

    iterator.close();

    return result;
    }

  protected List<Tuple> asList( Flow flow, Tap tap ) throws IOException
    {
    TupleEntryIterator iterator = flow.openTapForRead( tap );

    List<Tuple> result = asCollection( iterator, new ArrayList<Tuple>() );

    iterator.close();

    return result;
    }

  protected Set<Tuple> asSet( Flow flow, Tap tap ) throws IOException
    {
    TupleEntryIterator iterator = flow.openTapForRead( tap );

    Set<Tuple> result = asCollection( iterator, new HashSet<Tuple>() );

    iterator.close();

    return result;
    }

  protected <C extends Collection<Tuple>> C asCollection( Flow flow, Tap tap, C collection ) throws IOException
    {
    TupleEntryIterator iterator = flow.openTapForRead( tap );

    C result = asCollection( iterator, collection );

    iterator.close();

    return result;
    }

  protected <C extends Collection<Tuple>> C asCollection( TupleEntryIterator iterator, C result )
    {
    while( iterator.hasNext() )
      result.add( iterator.next().getTupleCopy() );

    return result;
    }
  }
