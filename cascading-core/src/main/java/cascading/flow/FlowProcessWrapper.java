/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import cascading.tap.Tap;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

/**
 *
 */
public class FlowProcessWrapper<Config> extends FlowProcess<Config>
  {
  final FlowProcess<Config> delegate;

  public static FlowProcess undelegate( FlowProcess flowProcess )
    {
    if( flowProcess instanceof FlowProcessWrapper )
      return ( (FlowProcessWrapper) flowProcess ).getDelegate();

    return flowProcess;
    }

  public FlowProcessWrapper( FlowProcess delegate )
    {
    this.delegate = delegate;
    }

  public FlowProcess getDelegate()
    {
    return delegate;
    }

  @Override
  public FlowProcess copyWith( Config object )
    {
    return delegate.copyWith( object );
    }

  @Override
  public String getID()
    {
    return delegate.getID();
    }

  @Override
  public FlowSession getCurrentSession()
    {
    return delegate.getCurrentSession();
    }

  @Override
  public void setCurrentSession( FlowSession currentSession )
    {
    delegate.setCurrentSession( currentSession );
    }

  @Override
  public int getNumProcessSlices()
    {
    return delegate.getNumProcessSlices();
    }

  @Override
  public int getCurrentSliceNum()
    {
    return delegate.getCurrentSliceNum();
    }

  @Override
  public Object getProperty( String key )
    {
    return delegate.getProperty( key );
    }

  @Override
  public Collection<String> getPropertyKeys()
    {
    return delegate.getPropertyKeys();
    }

  @Override
  public Object newInstance( String className )
    {
    return delegate.newInstance( className );
    }

  @Override
  public void keepAlive()
    {
    delegate.keepAlive();
    }

  @Override
  public void increment( Enum counter, long amount )
    {
    delegate.increment( counter, amount );
    }

  @Override
  public void increment( String group, String counter, long amount )
    {
    delegate.increment( group, counter, amount );
    }

  @Override
  public void setStatus( String status )
    {
    delegate.setStatus( status );
    }

  @Override
  public boolean isCounterStatusInitialized()
    {
    return delegate.isCounterStatusInitialized();
    }

  @Override
  public TupleEntryIterator openTapForRead( Tap tap ) throws IOException
    {
    return delegate.openTapForRead( tap );
    }

  @Override
  public TupleEntryCollector openTapForWrite( Tap tap ) throws IOException
    {
    return delegate.openTapForWrite( tap );
    }

  @Override
  public TupleEntryCollector openTrapForWrite( Tap trap ) throws IOException
    {
    return delegate.openTrapForWrite( trap );
    }

  @Override
  public TupleEntryCollector openSystemIntermediateForWrite() throws IOException
    {
    return delegate.openSystemIntermediateForWrite();
    }

  @Override
  public Config getConfigCopy()
    {
    return delegate.getConfigCopy();
    }

  @Override
  public Config copyConfig( Config jobConf )
    {
    return delegate.copyConfig( jobConf );
    }

  @Override
  public Map<String, String> diffConfigIntoMap( Config defaultConfig, Config updatedConfig )
    {
    return delegate.diffConfigIntoMap( defaultConfig, updatedConfig );
    }

  @Override
  public Config mergeMapIntoConfig( Config defaultConfig, Map<String, String> map )
    {
    return delegate.mergeMapIntoConfig( defaultConfig, map );
    }
  }
