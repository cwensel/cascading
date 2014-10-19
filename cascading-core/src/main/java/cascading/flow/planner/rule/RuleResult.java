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

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.graph.FlowElementGraph;
import cascading.flow.planner.rule.util.ResultTree;
import cascading.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.util.Util.formatDurationFromMillis;

/**
 *
 */
public class RuleResult
  {
  public static final int THRESHOLD_SECONDS = 10;

  private static final Logger LOG = LoggerFactory.getLogger( RuleResult.class );

  private Map<ProcessLevel, Set<ElementGraph>> levelParents = new HashMap<>();
  private ResultTree resultTree = new ResultTree();

  private long duration = 0;
  private Map<PlanPhase, Long> phaseDurations = new LinkedHashMap<>();
  private Map<PlanPhase, Map<String, Long>> ruleDurations = new LinkedHashMap<>();

  protected FlowElementGraph initialAssembly;
  private RuleRegistry registry;

  public RuleResult()
    {
    for( ProcessLevel level : ProcessLevel.values() )
      levelParents.put( level, new LinkedHashSet<ElementGraph>() );
    }

  public RuleResult( RuleRegistry registry )
    {
    this();

    this.registry = registry;
    }

  public RuleResult( FlowElementGraph initialAssembly )
    {
    this();

    initResult( initialAssembly );
    }

  public RuleRegistry getRegistry()
    {
    return registry;
    }

  public void initResult( FlowElementGraph initialAssembly )
    {
    this.initialAssembly = initialAssembly;

    setLevelResults( ProcessLevel.Assembly, initialAssembly, initialAssembly.copyGraph() );
    }

  public void setLevelResults( ProcessLevel level, Map<ElementGraph, List<? extends ElementGraph>> results )
    {
    for( Map.Entry<ElementGraph, List<? extends ElementGraph>> entry : results.entrySet() )
      setLevelResults( level, entry.getKey(), entry.getValue() );
    }

  public void setLevelResults( ProcessLevel level, ElementGraph parent, ElementGraph child )
    {
    setLevelResults( level, parent, Collections.singletonList( child ) );
    }

  public void setLevelResults( ProcessLevel level, ElementGraph parent, List<? extends ElementGraph> elementGraphs )
    {
    levelParents.get( level ).add( parent );
    resultTree.setChildren( parent, elementGraphs );
    }

  public Map<ElementGraph, List<? extends ElementGraph>> getLevelResults( ProcessLevel level )
    {
    Map<ElementGraph, List<? extends ElementGraph>> result = new HashMap<>();
    Set<? extends ElementGraph> parents = levelParents.get( level );

    if( !parents.isEmpty() )
      {
      for( ElementGraph parent : parents )
        result.put( parent, resultTree.getChildren( parent ) );

      return result;
      }

    // initialize the level
    Set<ElementGraph> top = levelParents.get( ProcessLevel.parent( level ) );

    for( ElementGraph parent : top )
      {
      List<? extends ElementGraph> children = resultTree.getChildren( parent );

      for( ElementGraph child : children )
        result.put( child, new ArrayList<ElementGraph>() );
      }

    return result;
    }

  public int[] getPathFor( ElementGraph parent, ElementGraph child )
    {
    return resultTree.getEdge( parent, child ).getOrdinals();
    }

  public int[] getPathFor( ElementGraph parent )
    {
    ResultTree.Path incomingEdge = resultTree.getIncomingEdge( parent );

    if( incomingEdge == null )
      return new int[ 0 ];

    return incomingEdge.getOrdinals();
    }

  public FlowElementGraph getInitialAssembly()
    {
    return initialAssembly;
    }

  public FlowElementGraph getAssemblyGraph()
    {
    Map<ElementGraph, List<? extends ElementGraph>> results = getLevelResults( ProcessLevel.Assembly );

    return (FlowElementGraph) Util.getFirst( results.get( getInitialAssembly() ) );
    }

  public Map<ElementGraph, List<? extends ElementGraph>> getAssemblyToStepGraphMap()
    {
    return getLevelResults( ProcessLevel.Step );
    }

  public Map<ElementGraph, List<? extends ElementGraph>> getStepToNodeGraphMap()
    {
    return getLevelResults( ProcessLevel.Node );
    }

  public Map<ElementGraph, List<? extends ElementGraph>> getNodeToPipelineGraphMap()
    {
    return getLevelResults( ProcessLevel.Pipeline );
    }

  public void setDuration( long begin, long end )
    {
    duration = end - begin;
    }

  public long getDuration()
    {
    return duration;
    }

  public void setPhaseDuration( PlanPhase phase, long begin, long end )
    {
    phaseDurations.put( phase, end - begin );
    }

  public void setRuleDuration( Rule rule, long begin, long end )
    {
    Map<String, Long> durations = ruleDurations.get( rule.getRulePhase() );

    if( durations == null )
      {
      durations = new LinkedHashMap<>();
      ruleDurations.put( rule.getRulePhase(), durations );
      }

    if( durations.containsKey( rule.getRuleName() ) )
      throw new IllegalStateException( "duplicate rule found: " + rule.getRuleName() );

    long duration = end - begin;

    // print these as we go
    if( duration > THRESHOLD_SECONDS * 1000 )
      LOG.info( "rule: {}, took longer than {} seconds: {}", rule.getRuleName(), THRESHOLD_SECONDS, formatDurationFromMillis( duration ) );

    durations.put( rule.getRuleName(), duration );
    }

  public void writeStats( PrintWriter writer )
    {
    writer.format( "duration\t%.03f\n", ( duration / 1000f ) );

    writer.println();

    for( PlanPhase phase : phaseDurations.keySet() )
      {
      long phaseDuration = phaseDurations.get( phase );

      writer.format( "%s\t%.03f\n", phase, ( phaseDuration / 1000f ) );

      Map<String, Long> rules = ruleDurations.get( phase );

      writer.println( "=======================" );

      if( rules != null )
        {
        for( String ruleName : rules.keySet() )
          {
          long ruleDuration = rules.get( ruleName );
          writer.format( "%s\t%.03f\n", ruleName, ( ruleDuration / 1000f ) );
          }
        }

      writer.println( "" );
      }
    }
  }
