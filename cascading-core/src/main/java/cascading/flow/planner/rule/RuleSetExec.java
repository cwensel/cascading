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

package cascading.flow.planner.rule;

import java.util.ArrayList;
import java.util.List;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.planner.FlowPlanner;
import cascading.flow.planner.PlannerContext;
import cascading.flow.planner.graph.FlowElementGraph;
import cascading.flow.planner.rule.util.TraceWriter;
import cascading.util.ProcessLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.util.Util.formatDurationFromMillis;

/**
 *
 */
public class RuleSetExec
  {
  private TraceWriter traceWriter;
  private FlowPlanner flowPlanner;
  private Flow flow;
  private RuleRegistrySet registrySet;
  private FlowDef flowDef;
  private FlowElementGraph flowElementGraph;

  public RuleSetExec( TraceWriter traceWriter, FlowPlanner flowPlanner, Flow flow, RuleRegistrySet registrySet, FlowDef flowDef, FlowElementGraph flowElementGraph )
    {
    this.traceWriter = traceWriter;
    this.flowPlanner = flowPlanner;
    this.flow = flow;
    this.registrySet = registrySet;
    this.flowDef = flowDef;
    this.flowElementGraph = flowElementGraph;
    }

  protected ProcessLogger getFlowLogger()
    {
    return (ProcessLogger) flow;
    }

  public RuleResult exec()
    {
    List<RuleResult> results = new ArrayList<>();

    for( RuleRegistry ruleRegistry : registrySet.ruleRegistries )
      {
      flowPlanner.configRuleRegistryDefaults( ruleRegistry );

      String registryName = ruleRegistry.getName();

      RuleExec ruleExec = new RuleExec( traceWriter, ruleRegistry );

      PlannerContext plannerContext = new PlannerContext( ruleRegistry, flowPlanner, flowDef, flow, traceWriter.isTransformTraceEnabled() );

      RuleResult ruleResult = ruleExec.exec( plannerContext, flowElementGraph );

      getFlowLogger().logInfo( "executed rule registry: {}, completed in: {}", registryName, formatDurationFromMillis( ruleResult.getDuration() ) );

      traceWriter.writeTracePlan( registryName, "completed-flow-element-graph", ruleResult.getAssemblyGraph() );

      traceWriter.writeStats( registryName, plannerContext, ruleResult );

      flowPlanner.verifyResult( ruleResult );

      results.add( ruleResult );
      }

    return results.get( 0 );
    }
  }
