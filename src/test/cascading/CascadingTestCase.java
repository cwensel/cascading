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

package cascading;

import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Pattern;

import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.Buffer;
import cascading.operation.ConcreteCall;
import cascading.operation.Filter;
import cascading.operation.Function;
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
public class CascadingTestCase extends TestCase
  {
  public CascadingTestCase()
    {
    }

  public CascadingTestCase( String string )
    {
    super( string );
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

    iterator.close();

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

  protected TupleListCollector invokeAggregator( Aggregator aggregator, Tuple[] argumentsArray, Fields resultFields )
    {
    TupleEntry[] entries = new TupleEntry[argumentsArray.length];

    for( int i = 0; i < argumentsArray.length; i++ )
      entries[ i ] = new TupleEntry( argumentsArray[ i ] );

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
    TupleEntry[] entries = new TupleEntry[argumentsArray.length];

    for( int i = 0; i < argumentsArray.length; i++ )
      entries[ i ] = new TupleEntry( argumentsArray[ i ] );

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
  }
