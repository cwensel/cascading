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

import cascading.flow.planner.iso.transformer.ElementFactory;
import cascading.util.LogUtil;

/**
 *
 */
public class RuleRegistry
  {
  private Map<String, ElementFactory> factories = new HashMap<>();
  private Map<PlanPhase, LinkedList<Rule>> rules = new HashMap<>();

  {
  for( PlanPhase phase : PlanPhase.values() )
    rules.put( phase, new LinkedList<Rule>() );
  }

  protected void enableDebugLogging()
    {
    LogUtil.setLog4jLevel( "cascading.flow.planner.iso.transformer", "DEBUG" );
    LogUtil.setLog4jLevel( "cascading.flow.planner.iso.assertion", "DEBUG" );
    LogUtil.setLog4jLevel( "cascading.flow.planner.iso.subgraph", "DEBUG" );
    }

  public ElementFactory addElementFactory( String name, ElementFactory elementFactory )
    {
    return factories.put( name, elementFactory );
    }

  public ElementFactory getElementFactory( String factoryName )
    {
    return factories.get( factoryName );
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
  }
