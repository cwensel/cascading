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

package cascading.flow.local.planner;

import java.util.Map;
import java.util.Properties;

import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlow;
import cascading.flow.planner.ElementGraph;
import cascading.flow.planner.FlowPlanner;
import cascading.flow.planner.FlowStepGraph;
import cascading.flow.planner.PlatformInfo;
import cascading.pipe.Pipe;
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
  public Properties getConfig()
    {
    return null;
    }

  @Override
  public PlatformInfo getPlatformInfo()
    {
    return new PlatformInfo( "local", "Concurrent, Inc.", Version.getRelease() );
    }

  @Override
  public void initialize( FlowConnector flowConnector, Map<Object, Object> properties )
    {
    super.initialize( flowConnector, properties );
    }

  protected LocalFlow createFlow( FlowDef flowDef )
    {
    return new LocalFlow( getPlatformInfo(), getProperties(), getConfig(), flowDef );
    }

  @Override
  public LocalFlow buildFlow( FlowDef flowDef )
    {
    ElementGraph elementGraph = null;

    try
      {
      // generic
      verifyAllTaps( flowDef );

      LocalFlow flow = createFlow( flowDef );

      Pipe[] tails = resolveTails( flowDef, flow );

      verifyAssembly( flowDef, tails );

      elementGraph = createElementGraph( flowDef, tails );

      // rules
      failOnLoneGroupAssertion( elementGraph );
      failOnMissingGroup( elementGraph );
      failOnMisusedBuffer( elementGraph );
      failOnGroupEverySplit( elementGraph );

      // generic
      elementGraph.removeUnnecessaryPipes(); // groups must be added before removing pipes
      elementGraph.resolveFields();

      // used for checkpointing
      elementGraph = flow.updateSchemes( elementGraph );

      FlowStepGraph flowStepGraph = new LocalStepGraph( flowDef.getName(), elementGraph );

      flow.initialize( elementGraph, flowStepGraph );

      return flow;
      }
    catch( Exception exception )
      {
      throw handleExceptionDuringPlanning( exception, elementGraph );
      }
    }

  @Override
  protected Tap makeTempTap( String prefix, String name )
    {
    return null;
    }
  }
