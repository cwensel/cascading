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

import java.util.Map;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.planner.ElementGraph;
import cascading.flow.planner.FlowPlanner;
import cascading.flow.planner.StepGraph;
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

      elementGraph = createElementGraph( flowDef.getTailsArray(), flowDef.getSources(), flowDef.getSinks(), flowDef.getTraps() );

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

      StepGraph stepGraph = new LocalStepGraph( flowDef.getName(), elementGraph, flowDef.getTrapsCopy() );

      return new LocalFlow( properties, null, flowDef, elementGraph, stepGraph );
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
