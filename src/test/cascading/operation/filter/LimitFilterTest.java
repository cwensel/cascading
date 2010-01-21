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
import cascading.flow.FlowProcess;
import cascading.flow.FlowSession;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.ConcreteCall;
import cascading.operation.Filter;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 *
 */
public class LimitFilterTest extends CascadingTestCase
  {
  private ConcreteCall operationCall;

  public LimitFilterTest()
    {
    super( "limit filter test" );
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

  private class TestFlowProcess extends HadoopFlowProcess
    {
    private int numMappers;
    private int numReducers;
    private int taskNum;

    public TestFlowProcess( boolean isMapper, int numMappers, int numReducers, int taskNum )
      {
      super( new FlowSession(), null, isMapper );
      this.numMappers = numMappers;
      this.numReducers = numReducers;
      this.taskNum = taskNum;
      }

    @Override
    public int getCurrentNumMappers()
      {
      return numMappers;
      }

    @Override
    public int getCurrentNumReducers()
      {
      return numReducers;
      }

    @Override
    public int getCurrentTaskNum()
      {
      return taskNum;
      }
    }

  public void testLimit()
    {
    int limit = 20;
    int tasks = 20;
    int values = 10;

    for( int currentLimit = 0; currentLimit < limit; currentLimit++ )
      {
      for( int currentTask = 1; currentTask < tasks; currentTask++ )
        {
        for( int currentValue = 1; currentValue < values; currentValue++ )
          {
          performLimitTest( currentLimit, currentTask, currentValue );
          }
        }
      }
    }

//  public void testHardLimit()
//    {
//    performLimitTest( 5, 3, 2 );
//    }

  private void performLimitTest( int limit, int tasks, int values )
    {
    Filter filter = new Limit( limit );

    int count = 0;

    for( int i = 0; i < tasks; i++ )
      {
      FlowProcess process = new TestFlowProcess( true, tasks, tasks, i );

      filter.prepare( process, operationCall );

      operationCall.setArguments( getEntry( new Tuple( 1 ) ) );

      for( int j = 0; j < values; j++ )
        {
        if( !filter.isRemove( process, operationCall ) )
          count++;
        }
      }

    String message = String.format( "limit:%d tasks:%d values:%d", limit, tasks, values );

    assertEquals( message, Math.min( limit, values * tasks ), count );
    }


  }