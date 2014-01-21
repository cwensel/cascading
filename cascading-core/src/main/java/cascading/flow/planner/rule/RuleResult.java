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
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import cascading.flow.planner.FlowElementGraph;
import cascading.flow.planner.graph.ElementGraph;

/**
 *
 */
public class RuleResult
  {
  private Map<PlanPhase, FlowElementGraph> elementsTrace = new HashMap<>();
  private List<ElementGraph> elementSubGraphs = new LinkedList<>();

  private long duration = 0;
  private Map<PlanPhase, Long> phaseDurations = new LinkedHashMap<>();
  private Map<PlanPhase, Map<String, Long>> ruleDurations = new LinkedHashMap<>();

  public void addResult( PlanPhase phase, FlowElementGraph elementGraph )
    {
    elementsTrace.put( phase, elementGraph );
    }

  public FlowElementGraph getResult( PlanPhase phase )
    {
    return elementsTrace.get( phase );
    }

  public Map<PlanPhase, FlowElementGraph> getElementsTrace()
    {
    return elementsTrace;
    }

  public void addSubGraphs( Collection<ElementGraph> subgraphs)
    {
    elementSubGraphs.addAll( subgraphs );
    }

  public void setDuration( long begin, long end )
    {
    duration = end - begin;
    }

  public List<ElementGraph> getElementSubGraphs()
    {
    return elementSubGraphs;
    }

  public void setPhaseDuration( PlanPhase phase, long begin, long end )
    {
    phaseDurations.put( phase, end - begin );
    }

  public void setRuleDuration( PlanPhase phase, Rule rule, long begin, long end )
    {
    Map<String, Long> durations = ruleDurations.get( phase );

    if( durations == null )
      {
      durations = new LinkedHashMap<>();
      ruleDurations.put( phase, durations );
      }

    if( durations.containsKey( rule.getRuleName() ) )
      throw new IllegalStateException( "duplicate rule found: " + rule.getRuleName() );

    durations.put( rule.getRuleName(), end - begin );
    }

  public void writeStats( PrintWriter writer )
    {
    writer.format( "duration\t%.03f\n", ( duration / 1000f ) );

    writer.println();

    for( PlanPhase phase : phaseDurations.keySet() )
      {
      long phaseDuration = phaseDurations.get( phase );

      if( phase.isTerminal() )
        writer.format( "%s\n", phase );
      else
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
