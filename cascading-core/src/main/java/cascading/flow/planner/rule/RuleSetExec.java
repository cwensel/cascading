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

package cascading.flow.planner.rule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.planner.FlowPlanner;
import cascading.flow.planner.PlannerContext;
import cascading.flow.planner.PlannerException;
import cascading.flow.planner.graph.FlowElementGraph;
import cascading.flow.planner.rule.util.TraceWriter;
import cascading.util.ProcessLogger;
import cascading.util.Util;

import static cascading.util.Util.formatDurationFromMillis;
import static java.util.Collections.synchronizedList;
import static java.util.Collections.synchronizedSet;

public class RuleSetExec
  {
  public static final int MAX_CONCURRENT_PLANNERS = 5;
  public static final int DEFAULT_TIMEOUT = 10 * 60;
  public static final Comparator<RuleResult> DEFAULT_PLAN_COMPARATOR = new Comparator<RuleResult>()
  {
  @Override
  public int compare( RuleResult lhs, RuleResult rhs )
    {
    int c = lhs.getNumSteps() - rhs.getNumSteps();

    if( c != 0 )
      return c;

    return lhs.getNumNodes() - rhs.getNumNodes();
    }

  @Override
  public String toString()
    {
    return "default comparator: selects plan with fewest steps and fewest nodes";
    }
  };

  private TraceWriter traceWriter;
  private FlowPlanner flowPlanner;
  private Flow flow;
  private RuleRegistrySet registrySet;
  private FlowDef flowDef;
  private FlowElementGraph flowElementGraph;

  Set<Callable> running;
  List<RuleResult> success;
  List<RuleResult> unsupported;
  List<RuleResult> illegal;
  List<RuleResult> interrupted;

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

  protected Comparator<RuleResult> getPlanComparator()
    {
    if( registrySet.getPlanComparator() != null )
      return registrySet.getPlanComparator();

    return DEFAULT_PLAN_COMPARATOR;
    }

  protected Comparator<RuleResult> getOrderComparator()
    {
    return new Comparator<RuleResult>()
    {
    @Override
    public int compare( RuleResult lhs, RuleResult rhs )
      {
      // preserver order of preference from rule registry if all things are equal
      return registrySet.indexOf( lhs.getRegistry() ) - registrySet.indexOf( rhs.getRegistry() );
      }
    };
    }

  public RuleResult exec()
    {
    running = synchronizedSet( new HashSet<Callable>() );
    success = synchronizedList( new ArrayList<RuleResult>() );
    unsupported = synchronizedList( new ArrayList<RuleResult>() );
    illegal = synchronizedList( new ArrayList<RuleResult>() );
    interrupted = synchronizedList( new ArrayList<RuleResult>() );

    List<Callable<RuleResult>> callables = createCallables();

    submitCallables( callables );

    notifyUnsupported();
    notifyIllegal();
    notifyInterrupted();

    return selectSuccess();
    }

  protected RuleResult execPlannerFor( RuleRegistry ruleRegistry )
    {
    flowPlanner.configRuleRegistryDefaults( ruleRegistry );

    String registryName = ruleRegistry.getName();

    RuleExec ruleExec = new RuleExec( traceWriter, ruleRegistry );

    PlannerContext plannerContext = new PlannerContext( ruleRegistry, flowPlanner, flowDef, flow, traceWriter.isTransformTraceEnabled() );

    RuleResult ruleResult = ruleExec.exec( plannerContext, flowElementGraph );

    getFlowLogger().logInfo( "executed rule registry: {}, completed as: {}, in: {}", registryName, ruleResult.getResultStatus(), formatDurationFromMillis( ruleResult.getDuration() ) );

    traceWriter.writeTracePlan( registryName, "completed-flow-element-graph", ruleResult.getAssemblyGraph() );
    traceWriter.writeStats( plannerContext, ruleResult );

    Exception plannerException;

    if( ruleResult.isSuccess() )
      plannerException = flowPlanner.verifyResult( ruleResult );
    else
      plannerException = ruleResult.getPlannerException(); // will be re-thrown below

    if( plannerException != null && plannerException instanceof PlannerException && ( (PlannerException) plannerException ).getElementGraph() != null )
      traceWriter.writeTracePlan( registryName, "failed-source-element-graph", ( (PlannerException) plannerException ).getElementGraph() );

    if( ruleResult.isSuccess() && plannerException != null )
      rethrow( plannerException );

    return ruleResult;
    }

  protected Set<Future<RuleResult>> submitCallables( List<Callable<RuleResult>> callables )
    {
    int size = Math.min( MAX_CONCURRENT_PLANNERS, callables.size() );

    ExecutorService executor = Executors.newFixedThreadPool( size );
    ExecutorCompletionService<RuleResult> completionService = new ExecutorCompletionService<>( executor );
    Set<Future<RuleResult>> futures = new HashSet<>();

    RuleRegistrySet.Select select = registrySet.getSelect();
    long totalDuration = registrySet.getPlannerTimeoutSec();
    long startAll = TimeUnit.MILLISECONDS.toSeconds( System.currentTimeMillis() );

    for( Callable<RuleResult> callable : callables )
      futures.add( completionService.submit( callable ) );

    executor.shutdown();

    try
      {
      boolean timedOut = false;

      while( !futures.isEmpty() )
        {
        Future<RuleResult> future = completionService.poll( totalDuration, TimeUnit.SECONDS );

        long currentDuration = TimeUnit.MILLISECONDS.toSeconds( System.currentTimeMillis() ) - startAll;

        totalDuration -= currentDuration;

        timedOut = future == null;

        if( timedOut )
          break;

        futures.remove( future );

        boolean success = binResult( future.get() );

        if( success && select == RuleRegistrySet.Select.FIRST )
          break;
        }

      if( !futures.isEmpty() )
        {
        if( timedOut )
          getFlowLogger().logWarn( "planner cancelling long running registries past timeout period: {}, see RuleRegistrySet#setPlannerTimeoutSec() to change timeout", formatDurationFromMillis( registrySet.getPlannerTimeoutSec() * 1000 ) );
        else
          getFlowLogger().logInfo( "first registry completed, planner cancelling remaining running registries: {}, successful: {}", futures.size(), success.size() );

        for( Future<RuleResult> current : futures )
          current.cancel( true );

        int timeout = 0;

        while( !running.isEmpty() && timeout < 60 )
          {
          Util.safeSleep( 500 );
          timeout++;
          }
        }
      }
    catch( InterruptedException exception )
      {
      getFlowLogger().logError( "planner thread interrupted", exception );

      rethrow( exception );
      }
    catch( ExecutionException exception )
      {
      rethrow( exception.getCause() );
      }

    return futures;
    }

  protected List<Callable<RuleResult>> createCallables()
    {
    List<Callable<RuleResult>> callables = new ArrayList<>();

    for( RuleRegistry ruleRegistry : registrySet.ruleRegistries )
      callables.add( createCallable( ruleRegistry ) );

    return callables;
    }

  private RuleResult selectSuccess()
    {
    if( success.isEmpty() )
      throw new IllegalStateException( "no planner results from registry set" );

    for( RuleResult ruleResult : success )
      getFlowLogger().logInfo( "rule registry: {}, supports assembly with steps: {}, nodes: {}", ruleResult.getRegistry().getName(), ruleResult.getNumSteps(), ruleResult.getNumNodes() );

    if( success.size() != 1 )
      {
      // sort is stable
      Collections.sort( success, getOrderComparator() );
      Collections.sort( success, getPlanComparator() );
      }

    RuleResult ruleResult = success.get( 0 );

    if( registrySet.getSelect() == RuleRegistrySet.Select.FIRST )
      getFlowLogger().logInfo( "rule registry: {}, result was selected as first successful", ruleResult.getRegistry().getName() );
    else if( registrySet.getSelect() == RuleRegistrySet.Select.COMPARED )
      getFlowLogger().logInfo( "rule registry: {}, result was selected using: \'{}\'", ruleResult.getRegistry().getName(), getPlanComparator().toString() );

    return ruleResult;
    }

  private void notifyUnsupported()
    {
    if( unsupported.isEmpty() )
      return;

    for( RuleResult ruleResult : unsupported )
      getFlowLogger().logInfo( "rule registry: {}, does not support assembly", ruleResult.getRegistry().getName() );

    if( !registrySet.isIgnoreFailed() || success.isEmpty() && illegal.isEmpty() && interrupted.isEmpty() )
      rethrow( unsupported.get( 0 ).getPlannerException() );
    }

  private void notifyIllegal()
    {
    if( illegal.isEmpty() )
      return;

    for( RuleResult ruleResult : illegal )
      getFlowLogger().logInfo( "rule registry: {}, found assembly to be malformed", ruleResult.getRegistry().getName() );

    if( !registrySet.isIgnoreFailed() || success.isEmpty() )
      rethrow( illegal.get( 0 ).getPlannerException() );
    }

  private void notifyInterrupted()
    {
    if( interrupted.isEmpty() )
      return;

    for( RuleResult ruleResult : interrupted )
      getFlowLogger().logInfo( "rule registry: {}, planned longer than default duration, was cancelled", ruleResult.getRegistry().getName() );

    if( interrupted.size() == registrySet.size() )
      throw new PlannerException( "planner registry timeout exceeded for all registries: " + formatDurationFromMillis( registrySet.getPlannerTimeoutSec() * 1000 ) );

    if( !registrySet.isIgnoreFailed() || success.isEmpty() )
      rethrow( interrupted.get( 0 ).getPlannerException() );
    }

  protected Callable<RuleResult> createCallable( final RuleRegistry ruleRegistry )
    {
    return new Callable<RuleResult>()
    {
    @Override
    public RuleResult call() throws Exception
      {
      running.add( this );

      try
        {
        return execPlannerFor( ruleRegistry );
        }
      finally
        {
        running.remove( this );
        }
      }
    };
    }

  protected boolean binResult( RuleResult ruleResult )
    {
    switch( ruleResult.getResultStatus() )
      {
      case SUCCESS:
        success.add( ruleResult );
        return true;

      case UNSUPPORTED:
        unsupported.add( ruleResult );
        break;

      case ILLEGAL:
        illegal.add( ruleResult );
        break;

      case INTERRUPTED:
        interrupted.add( ruleResult );
        break;
      }

    return false;
    }

  private void rethrow( Throwable throwable )
    {
    if( throwable instanceof Error )
      throw (Error) throwable;

    if( throwable instanceof RuntimeException )
      throw (RuntimeException) throwable;

    throw new PlannerException( throwable );
    }
  }
