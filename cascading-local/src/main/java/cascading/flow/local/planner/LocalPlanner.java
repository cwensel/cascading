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

package cascading.flow.local.planner;

import java.util.Properties;

import cascading.flow.FlowDef;
import cascading.flow.FlowStep;
import cascading.flow.local.LocalFlow;
import cascading.flow.local.LocalFlowStep;
import cascading.flow.planner.FlowPlanner;
import cascading.flow.planner.PlatformInfo;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.process.FlowNodeGraph;
import cascading.tap.Tap;
import cascading.util.Version;

/**
 *
 */
public class LocalPlanner extends FlowPlanner<LocalFlow, Properties>
  {
  public LocalPlanner()
    {
    }

  @Override
  public Properties getDefaultConfig()
    {
    return null;
    }

  @Override
  public PlatformInfo getPlatformInfo()
    {
    return new PlatformInfo( "local", "Concurrent, Inc.", Version.getRelease() );
    }

  protected LocalFlow createFlow( FlowDef flowDef )
    {
    return new LocalFlow( getPlatformInfo(), getDefaultProperties(), getDefaultConfig(), flowDef );
    }

  @Override
  public FlowStep<Properties> createFlowStep( int numSteps, int ordinal, ElementGraph stepElementGraph, FlowNodeGraph flowNodeGraph )
    {
    return new LocalFlowStep( "local", ordinal, stepElementGraph, flowNodeGraph );
    }

  @Override
  protected Tap makeTempTap( String prefix, String name )
    {
    return null;
    }
  }
