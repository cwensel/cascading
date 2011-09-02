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

package cascading.flow.local;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import cascading.flow.FlowProcess;
import cascading.flow.FlowSession;
import cascading.tap.Tap;
import cascading.tuple.SpillableTupleList;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

/**
 *
 */
public class LocalFlowProcess extends FlowProcess<Properties>
  {
  private final Properties config;
  private LocalStepStats stepStats;

  public LocalFlowProcess()
    {
    config = new Properties();
    }

  public LocalFlowProcess( FlowSession flowSession, Properties config )
    {
    super( flowSession );
    this.config = config;
    }

  public void setStepStats( LocalStepStats stepStats )
    {
    this.stepStats = stepStats;
    }

  @Override
  public int getNumConcurrentTasks()
    {
    return 0;
    }

  @Override
  public int getCurrentTaskNum()
    {
    return 0;
    }

  @Override
  public Object getProperty( String key )
    {
    return config.getProperty( key );
    }

  @Override
  public void keepAlive()
    {
    }

  @Override
  public void increment( Enum counter, int amount )
    {
    stepStats.increment( counter, amount );
    }

  @Override
  public void increment( String group, String counter, int amount )
    {
    increment( group, counter, amount );
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
    return tap.openForRead( this );
    }

  @Override
  public TupleEntryCollector openTapForWrite( Tap tap ) throws IOException
    {
    return tap.openForWrite( this );
    }

  @Override
  public TupleEntryCollector openTrapForWrite( Tap trap ) throws IOException
    {
    return trap.openForWrite( this );
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
  public FlowProcess copyWith( Properties object )
    {
    return null;
    }

  @Override
  public Properties getConfigCopy()
    {
    return new Properties( config );
    }

  @Override
  public Properties copyConfig( Properties config )
    {
    return new Properties( config );
    }

  @Override
  public Map<String, String> diffConfigIntoMap( Properties defaultConfig, Properties updatedConfig )
    {
    return null;
    }

  @Override
  public Properties mergeMapIntoConfig( Properties defaultConfig, Map<String, String> map )
    {
    return null;
    }
  }
