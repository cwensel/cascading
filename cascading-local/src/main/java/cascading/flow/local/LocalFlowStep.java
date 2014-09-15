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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import cascading.flow.FlowProcess;
import cascading.flow.local.planner.LocalFlowStepJob;
import cascading.flow.planner.BaseFlowStep;
import cascading.flow.planner.FlowStepJob;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.process.FlowNodeGraph;
import cascading.management.state.ClientState;
import cascading.property.ConfigDef;
import cascading.tap.Tap;

/** Class LocalFlowStep is the local mode implementation of {@link cascading.flow.FlowStep}. */
public class LocalFlowStep extends BaseFlowStep<Properties>
  {
  /** Map of Properties modified by each Tap's sourceConfInit/sinkConfInit */
  private final Map<Tap, Properties> tapProperties = new HashMap<Tap, Properties>();

  public LocalFlowStep( String name, int id, ElementGraph elementGraph, FlowNodeGraph flowNodeGraph )
    {
    super( name, id, elementGraph, flowNodeGraph );
    }

  @Override
  public Properties createInitializedConfig( FlowProcess<Properties> flowProcess, Properties parentConfig )
    {
    Properties currentProperties = parentConfig == null ? new Properties() : new Properties( parentConfig );

    initTaps( flowProcess, currentProperties, getSourceTaps(), false );
    initTaps( flowProcess, currentProperties, getSinkTaps(), true );
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
    initConfFromProcessConfigDef( getElementGraph(), getSetterFor( properties ) );
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
  protected FlowStepJob<Properties> createFlowStepJob( ClientState clientState, FlowProcess<Properties> flowProcess, Properties initializedStepConfig )
    {
    // localize a flow process
    flowProcess = new LocalFlowProcess( flowProcess.getCurrentSession(), initializedStepConfig );

    return new LocalFlowStepJob( clientState, (LocalFlowProcess) flowProcess, this );
    }

  public Map<Tap, Properties> getPropertiesMap()
    {
    return tapProperties;
    }
  }
