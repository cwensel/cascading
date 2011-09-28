/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import cascading.flow.FlowProcess;
import cascading.flow.planner.FlowStep;
import cascading.flow.planner.FlowStepJob;
import cascading.tap.Tap;

/**
 *
 */
public class LocalFlowStep extends FlowStep<Properties>
  {
  /** Field mapperTraps */
  private final Map<String, Tap> traps = new HashMap<String, Tap>();

  protected LocalFlowStep( String name, int id )
    {
    super( name, id );
    }

  @Override
  public Properties getInitializedConfig( FlowProcess<Properties> flowProcess, Properties parentConfig ) throws IOException
    {
    Properties currentProperties = parentConfig == null ? new Properties() : new Properties( parentConfig );

    // sets properties local to step
    if( hasProperties() )
      {
      for( Map.Entry entry : getProperties().entrySet() )
        currentProperties.put( entry.getKey().toString(), entry.getValue().toString() );
      }

    initTaps( flowProcess, currentProperties, getSourceMap() );
    initTaps( flowProcess, currentProperties, getSinkMap() );
    initTaps( flowProcess, currentProperties, getTrapMap() );

    return currentProperties;
    }

  protected void initTaps( FlowProcess<Properties> flowProcess, Properties conf, Map<String, Tap> taps ) throws IOException
    {
    if( !taps.isEmpty() )
      {
      Properties confCopy = flowProcess.copyConfig( conf );

      for( Tap tap : taps.values() )
        tap.sinkConfInit( flowProcess, confCopy );
      }
    }

  @Override
  public void clean( Properties config )
    {
    }

  @Override
  protected FlowStepJob createFlowStepJob( FlowProcess<Properties> flowProcess, Properties parentConfig )
    {
    setConf( flowProcess.getConfigCopy() );

    return new LocalFlowStepJob( createClientState( flowProcess ), (LocalFlowProcess) flowProcess, this );
    }

  public Map<String, Tap> getTrapMap()
    {
    return traps;
    }

  public Tap getTrap( String name )
    {
    return getTrapMap().get( name );
    }
  }
