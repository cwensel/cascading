/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

import java.io.IOException;
import java.util.Map;

import cascading.CascadingTestCase;
import cascading.flow.FlowProcess;
import cascading.flow.FlowSession;
import cascading.operation.ConcreteCall;
import cascading.operation.Filter;
import cascading.tap.Tap;
import cascading.test.PlatformTest;
import cascading.tuple.Fields;
import cascading.tuple.SpillableTupleList;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

/**
 *
 */
@PlatformTest(platforms = {"none"})
public class LimitFilterTest extends CascadingTestCase
  {
  private ConcreteCall operationCall;

  public LimitFilterTest()
    {
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

  private class TestFlowProcess extends FlowProcess<Object>
    {
    private int numTasks;
    private int taskNum;

    public TestFlowProcess( int numTasks, int taskNum )
      {
      super( new FlowSession() );
      this.numTasks = numTasks;
      this.taskNum = taskNum;
      }

    @Override
    public FlowProcess copyWith( Object object )
      {
      return null;
      }

    @Override
    public int getNumConcurrentTasks()
      {
      return numTasks;
      }

    @Override
    public int getCurrentTaskNum()
      {
      return taskNum;
      }

    @Override
    public Object getProperty( String key )
      {
      return null;
      }

    @Override
    public void keepAlive()
      {
      }

    @Override
    public void increment( Enum counter, int amount )
      {
      }

    @Override
    public void increment( String group, String counter, int amount )
      {
      }

    @Override
    public void setStatus( String status )
      {
      }

    @Override
    public boolean isCounterStatusInitialized()
      {
      return true;
      }

    @Override
    public TupleEntryIterator openTapForRead( Tap tap ) throws IOException
      {
      return null;
      }

    @Override
    public TupleEntryCollector openTapForWrite( Tap tap ) throws IOException
      {
      return null;
      }

    @Override
    public TupleEntryCollector openTrapForWrite( Tap trap ) throws IOException
      {
      return null;
      }

    @Override
    public TupleEntryCollector openSystemIntermediateForWrite() throws IOException
      {
      return null;
      }

    @Override
    public SpillableTupleList createSpillableTupleList()
      {
      return null;
      }

    @Override
    public Object getConfigCopy()
      {
      return null;
      }

    @Override
    public Object copyConfig( Object jobConf )
      {
      return null;
      }

    @Override
    public Map<String, String> diffConfigIntoMap( Object defaultConfig, Object updatedConfig )
      {
      return null;
      }

    @Override
    public Object mergeMapIntoConfig( Object defaultConfig, Map<String, String> map )
      {
      return null;
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
      FlowProcess process = new TestFlowProcess( tasks, i );

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