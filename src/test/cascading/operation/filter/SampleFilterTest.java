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
    super( "sample filter test" );
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