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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;

/**
 * Class RuleRegistrySet manages the set of {@link cascading.flow.planner.rule.RuleRegistry} instances that should be
 * applied via the {@link cascading.flow.planner.FlowPlanner} to the given assembly.
 * <p/>
 * The current RuleRegistrySet can be reached via {@link cascading.flow.FlowConnector#getRuleRegistrySet()}, or set with
 * the appropriate {@link cascading.flow.FlowConnector} constructor sub-class.
 * <p/>
 * RuleRegistrySet configuration is mutable to address different situations.
 * <p/>
 * During planner execution, all RuleRegistry instances are applied simultaneously within individual threads.
 * <p/>
 * If the planner duration exceeds {@link #getPlannerTimeoutSec()} (with default
 * {@link cascading.flow.planner.rule.RuleSetExec#DEFAULT_TIMEOUT}) any incomplete planner executions
 * will be cancelled.
 * <p/>
 * If no planner executions complete successfully within the timeout period, an exception will be thrown.
 * <p/>
 * If there are multiple successful completions, the default cost comparator,
 * {@link cascading.flow.planner.rule.RuleSetExec#DEFAULT_RESULT_COMPARATOR}, will be applied to find the lower
 * cost plan. Use {@link #setPlanComparator(java.util.Comparator)} to override.
 * <p/>
 * If all plans have equivalent costs, the plan corresponding to the first most RuleRegistry, as given to the
 * RuleRegistrySet, will be selected.
 * <p/>
 * If {@link #setSelect(cascading.flow.planner.rule.RuleRegistrySet.Select)} is set to
 * {@link cascading.flow.planner.rule.RuleRegistrySet.Select#FIRST}, the first RuleRegistry to complete will be used
 * regardless of cost ordering provided by the plan Comparator. All remaining running planner executions will be
 * cancelled.
 * <p/>
 * If {@link #isIgnoreFailed()} is {@code false}, if any planner execution is times out or fails, an exception will be
 * thrown.
 */
public class RuleRegistrySet
  {
  public enum Select
    {
      FIRST,
      COMPARED
    }

  int plannerTimeoutSec = RuleSetExec.DEFAULT_TIMEOUT;
  boolean ignoreFailed = true;
  Select select = Select.COMPARED;
  Comparator<RuleResult> planComparator = RuleSetExec.DEFAULT_RESULT_COMPARATOR;

  LinkedList<RuleRegistry> ruleRegistries = new LinkedList<>(); // preserve order, no duplicates

  public RuleRegistrySet( RuleRegistry... ruleRegistries )
    {
    this( Arrays.asList( ruleRegistries ) );
    }

  public RuleRegistrySet( Collection<RuleRegistry> ruleRegistries )
    {
    this.ruleRegistries.addAll( ruleRegistries );

    for( RuleRegistry ruleRegistry : ruleRegistries )
      {
      if( Collections.frequency( this.ruleRegistries, ruleRegistry ) > 1 )
        throw new IllegalArgumentException( "may not include duplicate registries" );
      }
    }

  public int getPlannerTimeoutSec()
    {
    return plannerTimeoutSec;
    }

  public void setPlannerTimeoutSec( int plannerTimeoutSec )
    {
    this.plannerTimeoutSec = plannerTimeoutSec;
    }

  public boolean isIgnoreFailed()
    {
    return ignoreFailed;
    }

  public void setIgnoreFailed( boolean ignoreFailed )
    {
    this.ignoreFailed = ignoreFailed;
    }

  public Select getSelect()
    {
    return select;
    }

  public void setSelect( Select select )
    {
    this.select = select;
    }

  public Comparator<RuleResult> getPlanComparator()
    {
    return planComparator;
    }

  public void setPlanComparator( Comparator planComparator )
    {
    this.planComparator = planComparator;
    }

  public int size()
    {
    return ruleRegistries.size();
    }

  public RuleRegistry findRegistryWith( String ruleName )
    {
    for( RuleRegistry ruleRegistry : ruleRegistries )
      {
      if( ruleRegistry.hasRule( ruleName ) )
        return ruleRegistry;
      }

    return null;
    }

  protected int indexOf( RuleRegistry ruleRegistry )
    {
    return ruleRegistries.indexOf( ruleRegistry );
    }
  }
