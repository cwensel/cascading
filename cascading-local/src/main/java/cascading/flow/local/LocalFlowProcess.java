/*
 * Copyright (c) 2007-2022 The Cascading Authors. All Rights Reserved.
 *
 * Project and contact information: https://cascading.wensel.net/
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

package cascading.flow.local;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.flow.FlowSession;
import cascading.stats.local.LocalStepStats;
import cascading.tap.Tap;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

/** Class LocalFlowProcess is the local mode implementation of {@link FlowProcess}. */
public class LocalFlowProcess extends FlowProcess<Properties>
  {
  private final Properties config;
  private LocalStepStats stepStats;

  public LocalFlowProcess()
    {
    config = new Properties();
    }

  public LocalFlowProcess( Properties config )
    {
    this.config = config;
    }

  public LocalFlowProcess( FlowSession flowSession, Properties config )
    {
    super( flowSession );
    this.config = config;
    }

  public LocalFlowProcess( LocalFlowProcess flowProcess, Properties properties )
    {
    super( flowProcess );
    this.config = properties;
    this.stepStats = flowProcess.stepStats;
    }

  public void setStepStats( LocalStepStats stepStats )
    {
    this.stepStats = stepStats;
    }

  @Override
  public int getNumProcessSlices()
    {
    return 1;
    }

  @Override
  public int getCurrentSliceNum()
    {
    return 0;
    }

  @Override
  public Object getProperty( String key )
    {
    return config.getProperty( key );
    }

  @Override
  public Collection<String> getPropertyKeys()
    {
    return Collections.unmodifiableSet( config.stringPropertyNames() );
    }

  @Override
  public Object newInstance( String className )
    {
    if( className == null || className.isEmpty() )
      return null;

    try
      {
      Class type = LocalFlowProcess.class.getClassLoader().loadClass( className );

      return type.newInstance();
      }
    catch( ClassNotFoundException exception )
      {
      throw new CascadingException( "unable to load class: " + className, exception );
      }
    catch( InstantiationException exception )
      {
      throw new CascadingException( "unable to instantiate class: " + className, exception );
      }
    catch( IllegalAccessException exception )
      {
      throw new CascadingException( "unable to access class: " + className, exception );
      }
    }

  @Override
  public void keepAlive()
    {
    }

  @Override
  public void increment( Enum counter, long amount )
    {
    if( stepStats != null )
      stepStats.increment( counter, amount );
    }

  @Override
  public void increment( String group, String counter, long amount )
    {
    if( stepStats != null )
      stepStats.increment( group, counter, amount );
    }

  @Override
  public long getCounterValue( Enum counter )
    {
    if( stepStats != null )
      return stepStats.getCounterValue( counter );

    return 0;
    }

  @Override
  public long getCounterValue( String group, String counter )
    {
    if( stepStats != null )
      return stepStats.getCounterValue( group, counter );

    return 0;
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
    return tap.openForWrite( this, null ); // do not honor sinkmode as this may be opened across tasks
    }

  @Override
  public TupleEntryCollector openTrapForWrite( Tap trap ) throws IOException
    {
    return trap.openForWrite( this, null ); // do not honor sinkmode as this may be opened across tasks
    }

  @Override
  public TupleEntryCollector openSystemIntermediateForWrite() throws IOException
    {
    return null;
    }

  @Override
  public FlowProcess copyWith( Properties object )
    {
    return new LocalFlowProcess( this, object );
    }

  @Override
  public Properties getConfig()
    {
    return config;
    }

  @Override
  public Properties getConfigCopy()
    {
    return new Properties( config );
    }

  @Override
  public <C> C copyConfig( C config )
    {
    return (C) new Properties( (Properties) config );
    }

  @Override
  public <C> Map<String, String> diffConfigIntoMap( C defaultConfig, C updatedConfig )
    {
    return null;
    }

  @Override
  public Properties mergeMapIntoConfig( Properties defaultConfig, Map<String, String> map )
    {
    Properties results = new Properties( defaultConfig );

    if( map == null )
      return results;

    for( Map.Entry<String, String> entry : map.entrySet() )
      results.put( entry.getKey(), entry.getValue() );

    return results;
    }
  }
