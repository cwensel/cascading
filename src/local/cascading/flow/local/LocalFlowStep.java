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
  public FlowStepJob createFlowStepJob( FlowProcess<Properties> flowProcess, Properties parentConfig )
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
