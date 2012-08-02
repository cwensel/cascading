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

package cascading.operation.filter;

import cascading.CascadingTestCase;
import cascading.operation.ConcreteCall;
import cascading.operation.Filter;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 *
 */
public class SampleFilterTest extends CascadingTestCase
  {
  private ConcreteCall operationCall;

  public SampleFilterTest()
    {
    super();
    }

  @Override
  protected void setUp() throws Exception
    {
    super.setUp();

    operationCall = new ConcreteCall();
    }

  private TupleEntry getEntry( Tuple tuple )
    {
    return new TupleEntry( Fields.size( tuple.size() ), tuple );
    }


  public void testSample()
    {
    for( double i = 0; i < 1; i = i + .01 )
      {
      performSampleTest( i, 100000 );
      }
    }

  private void performSampleTest( double sample, int values )
    {
    Filter filter = new Sample( sample );

    int count = 0;

    filter.prepare( null, operationCall );

    operationCall.setArguments( getEntry( new Tuple( 1 ) ) );

    for( int j = 0; j < values; j++ )
      {
      if( !filter.isRemove( null, operationCall ) )
        count++;
      }

    String message = String.format( "sample:%f values:%d", sample, values );

    assertEquals( message, sample, (double) count / values, 1 );
    }
  }