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

package cascading.flow.planner;

import java.util.HashMap;
import java.util.Map;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.planner.iso.transformer.ElementFactory;
import cascading.flow.planner.rule.RuleRegistry;
import cascading.operation.PlannerLevel;

/**
 *
 */
public class PlannerContext
  {
  public static final PlannerContext NULL = new PlannerContext();

  RuleRegistry ruleRegistry;
  FlowPlanner flowPlanner;
  FlowDef flowDef;
  Flow flow;
  String tracePath;

  public PlannerContext()
    {
    }

  public PlannerContext( RuleRegistry ruleRegistry )
    {
    this.ruleRegistry = ruleRegistry;
    }

  public PlannerContext( RuleRegistry ruleRegistry, FlowPlanner flowPlanner, FlowDef flowDef, Flow flow, String tracePath )
    {
    this.ruleRegistry = ruleRegistry;
    this.flowPlanner = flowPlanner;
    this.flowDef = flowDef;
    this.flow = flow;
    this.tracePath = tracePath;
    }

  public RuleRegistry getRuleRegistry()
    {
    return ruleRegistry;
    }

  public FlowPlanner getFlowPlanner()
    {
    return flowPlanner;
    }

  public FlowDef getFlowDef()
    {
    return flowDef;
    }

  public Flow getFlow()
    {
    return flow;
    }

  public String getTracePath()
    {
    return tracePath;
    }

  public boolean isTracingEnabled()
    {
    return tracePath != null;
    }

  public PlannerLevel getPlannerLevelFor( Class<? extends PlannerLevel> plannerLevelClass )
    {
    Map<Class<? extends PlannerLevel>, PlannerLevel> levels = new HashMap<>();

    addLevel( levels, flowPlanner.getDebugLevel( flowDef ) );
    addLevel( levels, flowPlanner.getAssertionLevel( flowDef ) );

    return levels.get( plannerLevelClass );
    }

  private void addLevel( Map<Class<? extends PlannerLevel>, PlannerLevel> levels, PlannerLevel level )
    {
    if( level != null )
      levels.put( level.getClass(), level );
    }

  public ElementFactory getElementFactoryFor( String factoryName )
    {
    return ruleRegistry.getElementFactory( factoryName );
    }
  }
