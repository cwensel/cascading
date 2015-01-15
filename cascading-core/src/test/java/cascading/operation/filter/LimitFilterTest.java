/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import cascading.CascadingTestCase;
import cascading.flow.FlowProcess;
import cascading.flow.FlowSession;
import cascading.operation.ConcreteCall;
import cascading.operation.Filter;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

/**
 *
 */
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
    public int getNumProcessSlices()
      {
      return numTasks;
      }

    @Override
    public int getCurrentSliceNum()
      {
      return taskNum;
      }

    @Override
    public Object getProperty( String key )
      {
      return null;
      }

    @Override
    public Collection<String> getPropertyKeys()
      {
      return null;
      }

    @Override
    public Object newInstance( String className )
      {
      return null;
      }

    @Override
    public void keepAlive()
      {
      }

    @Override
    public void increment( Enum counter, long amount )
      {
      }

    @Override
    public void increment( String group, String counter, long amount )
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