/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlow;
import cascading.flow.planner.ElementGraph;
import cascading.flow.planner.FlowPlanner;
import cascading.flow.planner.FlowStepGraph;
import cascading.tap.Tap;

/**
 *
 */
public class LocalPlanner extends FlowPlanner
  {
  public LocalPlanner()
    {
    }

  @Override
  public void initialize( FlowConnector flowConnector, Map<Object, Object> properties )
    {
    super.initialize( flowConnector, properties );
    }

  @Override
  public Flow buildFlow( FlowDef flowDef )
    {
    ElementGraph elementGraph = null;

    try
      {
      // generic
      verifyAssembly( flowDef );

      elementGraph = createElementGraph( flowDef );

      // rules
      failOnLoneGroupAssertion( elementGraph );
      failOnMissingGroup( elementGraph );
      failOnMisusedBuffer( elementGraph );
      failOnGroupEverySplit( elementGraph );

      // m/r specific
//      handleWarnEquivalentPaths( elementGraph );
//      handleSplit( elementGraph );
//      handleGroupPartitioning( elementGraph );
//      handleNonSafeOperations( elementGraph );

      // generic
      elementGraph.removeUnnecessaryPipes(); // groups must be added before removing pipes
      elementGraph.resolveFields();

      // m/r specific
//      handleAdjacentTaps( elementGraph );

      FlowStepGraph flowStepGraph = new LocalStepGraph( flowDef.getName(), elementGraph );

      return new LocalFlow( properties, null, flowDef, elementGraph, flowStepGraph );
      }
    catch( Exception exception )
      {
      throw handleExceptionDuringPlanning( exception, elementGraph );
      }
    }

  @Override
  protected Tap makeTempTap( String name )
    {
    return null;
    }
  }
