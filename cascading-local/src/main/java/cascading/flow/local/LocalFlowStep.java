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

package cascading.flow.local;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import cascading.flow.FlowProcess;
import cascading.flow.local.planner.LocalFlowStepJob;
import cascading.flow.planner.BaseFlowStep;
import cascading.flow.planner.FlowStepJob;
import cascading.property.ConfigDef;
import cascading.tap.Tap;

/** Class LocalFlowStep is the local mode implementation of {@link cascading.flow.FlowStep}. */
public class LocalFlowStep extends BaseFlowStep<Properties>
  {
  /** Field mapperTraps */
  private final Map<String, Tap> traps = new HashMap<String, Tap>();

  /** Map of Properties modified by each Tap's sourceConfInit/sinkConfInit */
  private final Map<Tap, Properties> tapProperties = new HashMap<Tap, Properties>();

  public LocalFlowStep( String name, int id )
    {
    super( name, id );
    }

  @Override
  public Properties getInitializedConfig( FlowProcess<Properties> flowProcess, Properties parentConfig )
    {
    Properties currentProperties = parentConfig == null ? new Properties() : new Properties( parentConfig );

    initTaps( flowProcess, currentProperties, getSources(), false );
    initTaps( flowProcess, currentProperties, getSinks(), true );
    initTaps( flowProcess, currentProperties, getTraps(), true );

    initFromProcessConfigDef( currentProperties );

    return currentProperties;
    }

  protected void initTaps( FlowProcess<Properties> flowProcess, Properties conf, Set<Tap> taps, boolean isSink )
    {
    if( !taps.isEmpty() )
      {
      for( Tap tap : taps )
        {
        Properties confCopy = flowProcess.copyConfig( conf );
        tapProperties.put( tap, confCopy ); // todo: store the diff, not the copy

        if( isSink )
          tap.sinkConfInit( flowProcess, confCopy );
        else
          tap.sourceConfInit( flowProcess, confCopy );
        }
      }
    }

  private void initFromProcessConfigDef( final Properties properties )
    {
    initConfFromProcessConfigDef( getSetterFor( properties ) );
    }

  private ConfigDef.Setter getSetterFor( final Properties properties )
    {
    return new ConfigDef.Setter()
    {
    @Override
    public String set( String key, String value )
      {
      String oldValue = get( key );

      properties.setProperty( key, value );

      return oldValue;
      }

    @Override
    public String update( String key, String value )
      {
      String oldValue = get( key );

      if( oldValue == null )
        properties.setProperty( key, value );
      else if( !oldValue.contains( value ) )
        properties.setProperty( key, oldValue + "," + value );

      return oldValue;
      }

    @Override
    public String get( String key )
      {
      String value = properties.getProperty( key );

      if( value == null || value.isEmpty() )
        return null;

      return value;
      }
    };
    }

  @Override
  public void clean( Properties config )
    {
    }

  @Override
  protected FlowStepJob<Properties> createFlowStepJob( FlowProcess<Properties> flowProcess, Properties parentConfig )
    {
    setConf( getInitializedConfig( flowProcess, parentConfig ) );

    flowProcess = new LocalFlowProcess( flowProcess.getCurrentSession(), getConfig() );

    return new LocalFlowStepJob( createClientState( flowProcess ), (LocalFlowProcess) flowProcess, this );
    }

  public Map<String, Tap> getTrapMap()
    {
    return traps;
    }

  public Map<Tap, Properties> getPropertiesMap()
    {
    return tapProperties;
    }

  @Override
  public Set<Tap> getTraps()
    {
    return Collections.unmodifiableSet( new HashSet<Tap>( traps.values() ) );
    }

  public Tap getTrap( String name )
    {
    return getTrapMap().get( name );
    }
  }
