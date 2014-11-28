/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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

package cascading.tuple;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.util.CloseableIterator;
import cascading.util.SingleValueCloseableIterator;

public class TupleEntrySchemeIteratorTest
  {
  @Test
  public void testHasNextWithException() throws Exception
    {
    FlowProcess<?> flowProcess = FlowProcess.NULL;
    Scheme<?, ?, ?, ?, ?> scheme = new MockedScheme();
    CloseableIterator<Object> inputIterator = new MockedSingleValueCloseableIterator( new Object() );
    TupleEntrySchemeIterator<?, ?> iterator = new TupleEntrySchemeIterator( flowProcess, scheme, inputIterator );

    assertTrue( iterator.hasNext() );
    assertTrue( iterator.hasNext() );
    }

  public class MockedScheme extends Scheme
    {
    private int callCount = 0;

    private static final long serialVersionUID = 1L;

    @Override
    public void sourceConfInit( FlowProcess flowProcess, Tap tap, Object conf )
      {
      }

    @Override
    public void sinkConfInit( FlowProcess flowProcess, Tap tap, Object conf )
      {
      }

    @Override
    public boolean source( FlowProcess flowProcess, SourceCall sourceCall ) throws IOException
      {
      // Mimicking a call to read a tuple that throws an exception and any subsequent call will just return false
      // indicating there are no more tuples left to read
      if( callCount == 0 )
        {
        callCount++;
        throw new IOException( "Error getting tuple" );
        }

      return false;
      }

    @Override
    public void sink( FlowProcess flowProcess, SinkCall sinkCall ) throws IOException
      {

      }
    }

  public class MockedSingleValueCloseableIterator extends SingleValueCloseableIterator<Object>
    {
    public MockedSingleValueCloseableIterator( Object value )
      {
      super( value );
      }

    @Override
    public void close() throws IOException
      {

      }
    }
  }
