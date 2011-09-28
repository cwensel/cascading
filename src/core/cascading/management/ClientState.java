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

package cascading.management;

import cascading.cascade.Cascade;
import cascading.flow.Flow;
import cascading.flow.planner.FlowStep;
import cascading.stats.CascadingStats;

/**
 *
 */
public abstract class ClientState extends BaseState
  {
  public static final String STATE_SERVICE_CLASS_PROPERTY = "cascading.management.state.service.classname";

  public static ClientState NULL = new ClientState()
  {
  @Override
  public void start( long time )
    {
    }

  @Override
  public void stop( long time )
    {
    }

  @Override
  String[] getContext( String group, String metric )
    {
    return new String[ 0 ];
    }

  @Override
  public void recordStats( CascadingStats stats )
    {
    }

  @Override
  public void recordFlowStep( FlowStep flowStep )
    {
    }

  @Override
  public void recordFlow( Flow flow )
    {
    }

  @Override
  public void recordCascade( Cascade cascade )
    {
    }
  };

  public ClientState()
    {
    }

  @Override
  String[] getContext( String group, String metric )
    {
    return asArray( group, metric );
    }

  public void setStatus( Enum status, long time )
    {
    setMetric( status, time );
    }

  public abstract void recordStats( CascadingStats stats );

  public abstract void recordFlowStep( FlowStep flowStep );

  public abstract void recordFlow( Flow flow );

  public abstract void recordCascade( Cascade cascade );

  public void record( String id, Object object )
    {
    store( id, object );
    }

  public void submit( long time )
    {
    setMetric( "state", "submit", time );
    }

  public void start( long time )
    {
    setMetric( "state", "start", time );
    }

  public void stop( long time )
    {
    setMetric( "state", "stop", time );
    }
  }
