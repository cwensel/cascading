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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import cascading.flow.planner.iso.transformer.ElementFactory;
import cascading.util.LogUtil;

/**
 * RuleRegistry contains all planner rules for a given platform. Where a rule is of type
 * {@link cascading.flow.planner.rule.Rule} and allows for either asserts, transforms, and partitioning
 * of pipe assembly process graphs.
 * <p/>
 * Process graphs are initially a standard Pipe assembly and connected Taps. And depending on the underlying platform
 * are partitioned into units of execution along the process hierarchy of
 * {@link cascading.flow.planner.rule.ProcessLevel#Assembly}, {@link cascading.flow.planner.rule.ProcessLevel#Step},
 * {@link cascading.flow.planner.rule.ProcessLevel#Node}, and {@link cascading.flow.planner.rule.ProcessLevel#Pipeline}.
 * <p/>
 * Where the Assembly is the user created pipe assembly and taps. The Steps are physical jobs executed by a platform.
 * Nodes are internal elements of work within a job (a Mapper or Reducer in the case of MapReduce). And Pipelines,
 * which are non-blocking streamed paths within a node. These can be optional.
 * <p/>
 * Rules rely on a 'language' for sub-graph pattern matching and can be applied against a given process type
 * (Assembly, or Step, etc), to test its correctness, or make changes to the graph. Or they can be used to partition a
 * large graph into a smaller graph, converting an Assembly into Steps.
 * <p/>
 * Debugging rule sets can be done by enabling system properties, see {@link cascading.flow.planner.FlowPlanner}.
 * <p/>
 * The {@link cascading.flow.planner.rule.RuleExec} class is responsible for executing on a given rule set.
 */
public class RuleRegistry
  {
  private Map<String, ElementFactory> factories = new HashMap<>();
  private Map<PlanPhase, LinkedList<Rule>> rules = new HashMap<>();

  {
  for( PlanPhase phase : PlanPhase.values() )
    rules.put( phase, new LinkedList<Rule>() );
  }

  private boolean resolveElementsEnabled = true;

  /**
   * Method enableDebugLogging forces log4j to emit DEBUG level stats for the planner classes.
   */
  protected void enableDebugLogging()
    {
    LogUtil.setLog4jLevel( "cascading.flow.planner.rule", "DEBUG" );
    LogUtil.setLog4jLevel( "cascading.flow.planner.iso.transformer", "DEBUG" );
    LogUtil.setLog4jLevel( "cascading.flow.planner.iso.assertion", "DEBUG" );
    LogUtil.setLog4jLevel( "cascading.flow.planner.iso.subgraph", "DEBUG" );
    LogUtil.setLog4jLevel( "cascading.flow.planner.iso.finder", "DEBUG" );
    }

  /**
   * Adds the named elementFactory if it does not already exist.
   *
   * @param name
   * @param elementFactory
   * @return true if the factory was added
   */
  public boolean addDefaultElementFactory( String name, ElementFactory elementFactory )
    {
    if( hasElementFactory( name ) )
      return false;

    factories.put( name, elementFactory );

    return true;
    }

  public ElementFactory addElementFactory( String name, ElementFactory elementFactory )
    {
    return factories.put( name, elementFactory );
    }

  public ElementFactory getElementFactory( String factoryName )
    {
    return factories.get( factoryName );
    }

  public boolean hasElementFactory( String factoryName )
    {
    return factories.containsKey( factoryName );
    }

  public LinkedList<Rule> getRulesFor( PlanPhase phase )
    {
    return rules.get( phase );
    }

  public boolean addRule( Rule rule )
    {
    if( rule.getRulePhase() == null )
      throw new IllegalArgumentException( "rule must have a rule phase: " + rule.getRuleName() );

    return rules.get( rule.getRulePhase() ).add( rule );
    }

  public boolean hasRule( String ruleName )
    {
    for( Map.Entry<PlanPhase, LinkedList<Rule>> entry : rules.entrySet() )
      {
      for( Rule rule : entry.getValue() )
        {
        if( rule.getRuleName().equals( ruleName ) )
          return true;
        }
      }

    return false;
    }

  public void setResolveElementsEnabled( boolean resolveElementsEnabled )
    {
    this.resolveElementsEnabled = resolveElementsEnabled;
    }

  public boolean enabledResolveElements()
    {
    return resolveElementsEnabled;
    }

  public Set<ProcessLevel> getProcessLevels()
    {
    Set<ProcessLevel> processLevels = new TreeSet<>();

    for( PlanPhase planPhase : rules.keySet() )
      {
      // only phases that rules were registered for
      if( !rules.get( planPhase ).isEmpty() )
        processLevels.add( planPhase.getLevel() );
      }

    return processLevels;
    }

  public String getName()
    {
    return getClass().getSimpleName();
    }
  }
