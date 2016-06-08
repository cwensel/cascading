/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
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
import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.planner.iso.transformer.ElementFactory;
import cascading.flow.planner.rule.RuleRegistry;
import cascading.operation.PlannerLevel;
import cascading.property.PropertyUtil;
import cascading.util.ProcessLogger;

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
  boolean isTransformTracingEnabled = false;
  private Map properties;

  public PlannerContext()
    {
    this.properties = new Properties();
    }

  public PlannerContext( RuleRegistry ruleRegistry )
    {
    this.ruleRegistry = ruleRegistry;
    }

  public PlannerContext( RuleRegistry ruleRegistry, FlowPlanner flowPlanner, FlowDef flowDef, Flow flow, boolean isTransformTracingEnabled )
    {
    this.ruleRegistry = ruleRegistry;
    this.flowPlanner = flowPlanner;
    this.flowDef = flowDef;
    this.flow = flow;
    this.isTransformTracingEnabled = isTransformTracingEnabled;

    if( flowPlanner != null )
      this.properties = flowPlanner.getDefaultProperties();
    else
      this.properties = new Properties();
    }

  public String getStringProperty( String property )
    {
    return PropertyUtil.getStringProperty( System.getProperties(), properties, property );
    }

  public int getIntProperty( String property, int defaultValue )
    {
    return PropertyUtil.getIntProperty( System.getProperties(), properties, property, defaultValue );
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

  public ProcessLogger getLogger()
    {
    Flow flow = getFlow();

    if( flow == null )
      return ProcessLogger.NULL;

    return (ProcessLogger) flow;
    }

  public boolean isTransformTracingEnabled()
    {
    return isTransformTracingEnabled;
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
