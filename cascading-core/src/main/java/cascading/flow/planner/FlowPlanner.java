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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import cascading.flow.AssemblyPlanner;
import cascading.flow.BaseFlow;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowConnectorProps;
import cascading.flow.FlowDef;
import cascading.flow.FlowElement;
import cascading.flow.Flows;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.graph.FlowElementGraph;
import cascading.flow.planner.process.FlowNodeFactory;
import cascading.flow.planner.process.FlowStepFactory;
import cascading.flow.planner.process.FlowStepGraph;
import cascading.flow.planner.rule.ProcessLevel;
import cascading.flow.planner.rule.RuleRegistry;
import cascading.flow.planner.rule.RuleRegistrySet;
import cascading.flow.planner.rule.RuleResult;
import cascading.flow.planner.rule.RuleSetExec;
import cascading.flow.planner.rule.transformer.IntermediateTapElementFactory;
import cascading.flow.planner.rule.util.TraceWriter;
import cascading.operation.AssertionLevel;
import cascading.operation.DebugLevel;
import cascading.pipe.Checkpoint;
import cascading.pipe.OperatorException;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.property.ConfigDef;
import cascading.property.PropertyUtil;
import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.Fields;
import cascading.util.Update;
import cascading.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.util.Util.*;
import static java.util.Arrays.asList;

/**
 * Class FlowPlanner is the base class for all planner implementations.
 * <p/>
 * This planner support tracing execution of each rule. See the appropriate properties on this
 * class to enable.
 */
public abstract class FlowPlanner<F extends BaseFlow, Config>
  {
  /**
   * Enables the planner to write out basic planner information including the initial element-graph,
   * completed element-graph, and the completed step-graph dot files.
   */
  public static final String TRACE_PLAN_PATH = "cascading.planner.plan.path";

  /**
   * Enables the planner to write out detail level planner information for each rule, including recursive
   * transforms.
   * <p/>
   * Use this to debug rules. This does increase overhead during planning.
   */
  public static final String TRACE_PLAN_TRANSFORM_PATH = "cascading.planner.plan.transforms.path";

  /**
   * Enables the planner to write out planner statistics for each planner phase and rule.
   */
  public static final String TRACE_STATS_PATH = "cascading.planner.stats.path";

  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( FlowPlanner.class );

  /** Field properties */
  protected Map<Object, Object> defaultProperties;

  protected String checkpointTapRootPath = null;

  /** Field assertionLevel */
  protected AssertionLevel defaultAssertionLevel;
  /** Field debugLevel */
  protected DebugLevel defaultDebugLevel;

  /**
   * Method getAssertionLevel returns the configured target planner {@link cascading.operation.AssertionLevel}.
   *
   * @param properties of type Map<Object, Object>
   * @return AssertionLevel the configured AssertionLevel
   */
  static AssertionLevel getAssertionLevel( Map<Object, Object> properties )
    {
    String assertionLevel = PropertyUtil.getProperty( properties, "cascading.flowconnector.assertionlevel", AssertionLevel.STRICT.name() );

    return AssertionLevel.valueOf( assertionLevel );
    }

  /**
   * Method getDebugLevel returns the configured target planner {@link cascading.operation.DebugLevel}.
   *
   * @param properties of type Map<Object, Object>
   * @return DebugLevel the configured DebugLevel
   */
  static DebugLevel getDebugLevel( Map<Object, Object> properties )
    {
    String debugLevel = PropertyUtil.getProperty( properties, "cascading.flowconnector.debuglevel", DebugLevel.DEFAULT.name() );

    return DebugLevel.valueOf( debugLevel );
    }

  {
  Update.registerPlanner( getClass() );
  }

  public Map<Object, Object> getDefaultProperties()
    {
    return defaultProperties;
    }

  public abstract Config getDefaultConfig();

  public abstract PlannerInfo getPlannerInfo( String name );

  public abstract PlatformInfo getPlatformInfo();

  public void initialize( FlowConnector flowConnector, Map<Object, Object> properties )
    {
    this.defaultProperties = properties;
    this.defaultAssertionLevel = getAssertionLevel( properties );
    this.defaultDebugLevel = getDebugLevel( properties );
    }

  public F buildFlow( FlowDef flowDef, RuleRegistrySet ruleRegistrySet )
    {
    FlowElementGraph flowElementGraph = null;

    try
      {
      flowDef = normalizeTaps( flowDef );

      verifyAllTaps( flowDef );

      F flow = createFlow( flowDef );

      Pipe[] tails = resolveTails( flowDef, flow );

      verifyAssembly( flowDef, tails );

      flowElementGraph = createFlowElementGraph( flowDef, tails );

      TraceWriter traceWriter = new TraceWriter( flow );
      RuleSetExec ruleSetExec = new RuleSetExec( traceWriter, this, flow, ruleRegistrySet, flowDef, flowElementGraph );

      RuleResult ruleResult = ruleSetExec.exec();

      traceWriter.writeTracePlan( null, "0-initial-flow-element-graph", flowElementGraph );

      FlowElementGraph finalFlowElementGraph = ruleResult.getAssemblyGraph();

      finalFlowElementGraph = flow.updateSchemes( finalFlowElementGraph );

      Map<ElementGraph, List<? extends ElementGraph>> stepToNodes = ruleResult.getStepToNodeGraphMap();
      Map<ElementGraph, List<? extends ElementGraph>> nodeToPipeline = ruleResult.getNodeToPipelineGraphMap();

      FlowStepGraph flowStepGraph = new FlowStepGraph( getFlowStepFactory(), finalFlowElementGraph, stepToNodes, nodeToPipeline );

      traceWriter.writeFinal( "1-final-flow-registry", ruleResult );
      traceWriter.writeTracePlan( null, "2-final-flow-element-graph", finalFlowElementGraph );
      traceWriter.writeTracePlan( null, "3-final-flow-step-graph", flowStepGraph );
      traceWriter.writeTracePlanSteps( "4-final-flow-steps", flowStepGraph );

      flow.setPlannerInfo( getPlannerInfo( ruleResult.getRegistry().getName() ) );

      flow.initialize( finalFlowElementGraph, flowStepGraph );

      return flow;
      }
    catch( Exception exception )
      {
      throw handleExceptionDuringPlanning( flowDef, exception, flowElementGraph );
      }
    }

  protected abstract F createFlow( FlowDef flowDef );

  public abstract FlowStepFactory<Config> getFlowStepFactory();

  public FlowNodeFactory getFlowNodeFactory()
    {
    return new BaseFlowNodeFactory();
    }

  public void configRuleRegistryDefaults( RuleRegistry ruleRegistry )
    {

    }

  protected Pipe[] resolveTails( FlowDef flowDef, F flow )
    {
    Pipe[] tails = flowDef.getTailsArray();

    tails = resolveAssemblyPlanners( flowDef, flow, tails );

    return tails;
    }

  protected Pipe[] resolveAssemblyPlanners( FlowDef flowDef, Flow flow, Pipe[] pipes )
    {
    List<Pipe> tails = Arrays.asList( pipes );

    List<AssemblyPlanner> assemblyPlanners = flowDef.getAssemblyPlanners();

    for( AssemblyPlanner assemblyPlanner : assemblyPlanners )
      {
      tails = assemblyPlanner.resolveTails( new AssemblyPlannerContext( flowDef, flow, tails ) );

      if( tails.isEmpty() )
        throw new PlannerException( "assembly planner: " + assemblyPlanner + ", returned zero tails" );

      tails = Collections.unmodifiableList( tails );
      }

    return tails.toArray( new Pipe[ tails.size() ] );
    }

  protected void verifyAssembly( FlowDef flowDef, Pipe[] tails )
    {
    verifyPipeAssemblyEndPoints( flowDef, tails );
    verifyTraps( flowDef, tails );
    verifyCheckpoints( flowDef, tails );
    }

  protected void verifyAllTaps( FlowDef flowDef )
    {
    verifySourceNotSinks( flowDef.getSources(), flowDef.getSinks() );

    verifyTaps( flowDef.getSources(), true, true );
    verifyTaps( flowDef.getSinks(), false, true );
    verifyTaps( flowDef.getTraps(), false, false );

    // are both sources and sinks
    verifyTaps( flowDef.getCheckpoints(), true, false );
    verifyTaps( flowDef.getCheckpoints(), false, false );
    }

  protected FlowElementGraph createFlowElementGraph( FlowDef flowDef, Pipe[] flowTails )
    {
    Map<String, Tap> sources = flowDef.getSourcesCopy();
    Map<String, Tap> sinks = flowDef.getSinksCopy();
    Map<String, Tap> traps = flowDef.getTrapsCopy();
    Map<String, Tap> checkpoints = flowDef.getCheckpointsCopy();

    checkpointTapRootPath = makeCheckpointRootPath( flowDef );

    return new FlowElementGraph( getPlatformInfo(), flowTails, sources, sinks, traps, checkpoints, checkpointTapRootPath != null );
    }

  private FlowDef normalizeTaps( FlowDef flowDef )
    {
    Set<Tap> taps = new HashSet<>();

    Map<String, Tap> sources = flowDef.getSourcesCopy();
    Map<String, Tap> sinks = flowDef.getSinksCopy();
    Map<String, Tap> traps = flowDef.getTrapsCopy();
    Map<String, Tap> checkpoints = flowDef.getCheckpointsCopy();

    boolean sourcesHasDupes = addTaps( sources, taps );
    boolean sinksHasDupes = addTaps( sinks, taps );
    boolean trapsHasDupes = addTaps( traps, taps );
    boolean checkpointsHasDupes = addTaps( checkpoints, taps );

    if( sourcesHasDupes )
      normalize( taps, sources );

    if( sinksHasDupes )
      normalize( taps, sinks );

    if( trapsHasDupes )
      normalize( taps, traps );

    if( checkpointsHasDupes )
      normalize( taps, checkpoints );

    return Flows.copy( flowDef, sources, sinks, traps, checkpoints );
    }

  private boolean addTaps( Map<String, Tap> current, Set<Tap> taps )
    {
    int size = taps.size();

    taps.addAll( current.values() );

    // if all the added values are not unique, taps.size will be less than original size + num tap instances
    return size + current.size() != taps.size();
    }

  private void normalize( Set<Tap> taps, Map<String, Tap> current )
    {
    for( Tap tap : taps )
      {
      for( Map.Entry<String, Tap> entry : current.entrySet() )
        {
        if( entry.getValue().equals( tap ) ) // force equivalent instance to being the same instance
          entry.setValue( tap );
        }
      }
    }

  private String makeCheckpointRootPath( FlowDef flowDef )
    {
    String flowName = flowDef.getName();
    String runID = flowDef.getRunID();

    if( runID == null )
      return null;

    if( flowName == null )
      throw new PlannerException( "flow name is required when providing a run id" );

    return flowName + "/" + runID;
    }

  protected void verifySourceNotSinks( Map<String, Tap> sources, Map<String, Tap> sinks )
    {
    Collection<Tap> sourcesSet = sources.values();

    for( Tap tap : sinks.values() )
      {
      if( sourcesSet.contains( tap ) )
        throw new PlannerException( "tap may not be used as both source and sink in the same Flow: " + tap );
      }
    }

  /**
   * Method verifyTaps ...
   *
   * @param taps          of type Map<String, Tap>
   * @param areSources    of type boolean
   * @param mayNotBeEmpty of type boolean
   */
  protected void verifyTaps( Map<String, Tap> taps, boolean areSources, boolean mayNotBeEmpty )
    {
    if( mayNotBeEmpty && taps.isEmpty() )
      throw new PlannerException( ( areSources ? "source" : "sink" ) + " taps are required" );

    for( String tapName : taps.keySet() )
      {
      if( areSources && !taps.get( tapName ).isSource() )
        throw new PlannerException( "tap named: '" + tapName + "', cannot be used as a source: " + taps.get( tapName ) );
      else if( !areSources && !taps.get( tapName ).isSink() )
        throw new PlannerException( "tap named: '" + tapName + "', cannot be used as a sink: " + taps.get( tapName ) );
      }
    }

  /**
   * Method verifyEndPoints verifies
   * <p/>
   * there aren't dupe names in heads or tails.
   * all the sink and source tap names match up with tail and head pipes
   */
  // todo: force dupe names to throw exceptions
  protected void verifyPipeAssemblyEndPoints( FlowDef flowDef, Pipe[] flowTails )
    {
    Set<String> tapNames = new HashSet<String>();

    tapNames.addAll( flowDef.getSources().keySet() );
    tapNames.addAll( flowDef.getSinks().keySet() );

    // handle tails
    Set<Pipe> tails = new HashSet<Pipe>();
    Set<String> tailNames = new HashSet<String>();

    for( Pipe pipe : flowTails )
      {
      if( pipe instanceof SubAssembly )
        {
        for( Pipe tail : ( (SubAssembly) pipe ).getTails() )
          {
          String tailName = tail.getName();

          if( !tapNames.contains( tailName ) )
            throw new PlannerException( tail, "pipe name not found in either sink or source map: '" + tailName + "'" );

          if( tailNames.contains( tailName ) && !tails.contains( tail ) )
            throw new PlannerException( pipe, "duplicate tail name found: " + tailName );

          tailNames.add( tailName );
          tails.add( tail );
          }
        }
      else
        {
        String tailName = pipe.getName();

        if( !tapNames.contains( tailName ) )
          throw new PlannerException( pipe, "pipe name not found in either sink or source map: '" + tailName + "'" );

        if( tailNames.contains( tailName ) && !tails.contains( pipe ) )
          throw new PlannerException( pipe, "duplicate tail name found: " + tailName );

        tailNames.add( tailName );
        tails.add( pipe );
        }
      }

    tailNames.removeAll( flowDef.getSinks().keySet() );
    Set<String> remainingSinks = new HashSet<String>( flowDef.getSinks().keySet() );
    remainingSinks.removeAll( tailNames );

    if( tailNames.size() != 0 )
      throw new PlannerException( "not all tail pipes bound to sink taps, remaining tail pipe names: [" + join( quote( tailNames, "'" ), ", " ) + "], remaining sink tap names: [" + join( quote( remainingSinks, "'" ), ", " ) + "]" );

    // unlike heads, pipes can input to another pipe and simultaneously be a sink
    // so there is no way to know all the intentional tails, so they aren't listed below in the exception
    remainingSinks = new HashSet<String>( flowDef.getSinks().keySet() );
    remainingSinks.removeAll( asList( Pipe.names( flowTails ) ) );

    if( remainingSinks.size() != 0 )
      throw new PlannerException( "not all sink taps bound to tail pipes, remaining sink tap names: [" + join( quote( remainingSinks, "'" ), ", " ) + "]" );

    // handle heads
    Set<Pipe> heads = new HashSet<Pipe>();
    Set<String> headNames = new HashSet<String>();

    for( Pipe pipe : flowTails )
      {
      for( Pipe head : pipe.getHeads() )
        {
        String headName = head.getName();

        if( !tapNames.contains( headName ) )
          throw new PlannerException( head, "pipe name not found in either sink or source map: '" + headName + "'" );

        if( headNames.contains( headName ) && !heads.contains( head ) )
          LOG.warn( "duplicate head name found, not an error but heads should have unique names: '{}'", headName );

        headNames.add( headName );
        heads.add( head );
        }
      }

    Set<String> allHeadNames = new HashSet<String>( headNames );
    headNames.removeAll( flowDef.getSources().keySet() );
    Set<String> remainingSources = new HashSet<String>( flowDef.getSources().keySet() );
    remainingSources.removeAll( headNames );

    if( headNames.size() != 0 )
      throw new PlannerException( "not all head pipes bound to source taps, remaining head pipe names: [" + join( quote( headNames, "'" ), ", " ) + "], remaining source tap names: [" + join( quote( remainingSources, "'" ), ", " ) + "]" );

    remainingSources = new HashSet<String>( flowDef.getSources().keySet() );
    remainingSources.removeAll( allHeadNames );

    if( remainingSources.size() != 0 )
      throw new PlannerException( "not all source taps bound to head pipes, remaining source tap names: [" + join( quote( remainingSources, "'" ), ", " ) + "], remaining head pipe names: [" + join( quote( headNames, "'" ), ", " ) + "]" );

    }

  protected void verifyTraps( FlowDef flowDef, Pipe[] flowTails )
    {
    verifyNotSourcesSinks( flowDef.getTraps(), flowDef.getSources(), flowDef.getSinks(), "trap" );

    Set<String> names = new HashSet<String>( asList( Pipe.names( flowTails ) ) );

    for( String name : flowDef.getTraps().keySet() )
      {
      if( !names.contains( name ) )
        throw new PlannerException( "trap name not found in assembly: '" + name + "'" );
      }
    }

  protected void verifyCheckpoints( FlowDef flowDef, Pipe[] flowTails )
    {
    verifyNotSourcesSinks( flowDef.getCheckpoints(), flowDef.getSources(), flowDef.getSinks(), "checkpoint" );

    for( Tap checkpointTap : flowDef.getCheckpoints().values() )
      {
      Scheme scheme = checkpointTap.getScheme();

      if( scheme.getSourceFields().equals( Fields.UNKNOWN ) && scheme.getSinkFields().equals( Fields.ALL ) )
        continue;

      throw new PlannerException( "checkpoint tap scheme must be undeclared, source fields must be UNKNOWN, and sink fields ALL, got: " + scheme.toString() );
      }

    Set<String> names = new HashSet<String>( asList( Pipe.names( flowTails ) ) );

    for( String name : flowDef.getCheckpoints().keySet() )
      {
      if( !names.contains( name ) )
        throw new PlannerException( "named checkpoint declared in FlowDef, but no named branch found in pipe assembly: '" + name + "'" );

      Set<Pipe> pipes = new HashSet<Pipe>( asList( Pipe.named( name, flowTails ) ) );

      int count = 0;

      for( Pipe pipe : pipes )
        {
        if( pipe instanceof Checkpoint )
          count++;
        }

      if( count == 0 )
        throw new PlannerException( "no checkpoint pipe with branch name found in pipe assembly: '" + name + "'" );

      if( count > 1 )
        throw new PlannerException( "more than one checkpoint pipe with branch name found in pipe assembly: '" + name + "'" );
      }
    }

  private void verifyNotSourcesSinks( Map<String, Tap> taps, Map<String, Tap> sources, Map<String, Tap> sinks, String role )
    {
    Collection<Tap> sourceTaps = sources.values();
    Collection<Tap> sinkTaps = sinks.values();

    for( Tap tap : taps.values() )
      {
      if( sourceTaps.contains( tap ) )
        throw new PlannerException( "tap may not be used as both a " + role + " and a source in the same Flow: " + tap );

      if( sinkTaps.contains( tap ) )
        throw new PlannerException( "tap may not be used as both a " + role + " and a sink in the same Flow: " + tap );
      }
    }

  /**
   * If there are rules for a given {@link cascading.flow.planner.rule.ProcessLevel} on the current platform
   * there must be sub-graphs partitioned at that level.
   */
  public Exception verifyResult( RuleResult ruleResult )
    {
    try
      {
      verifyResultInternal( ruleResult );
      }
    catch( Exception exception )
      {
      return exception;
      }

    return null;
    }

  protected void verifyResultInternal( RuleResult ruleResult )
    {
    Set<ProcessLevel> processLevels = getReverseOrderedProcessLevels( ruleResult );

    for( ProcessLevel processLevel : processLevels )
      {
      String registryName = ruleResult.getRegistry().getName();

      switch( processLevel )
        {
        case Assembly:

          FlowElementGraph finalFlowElementGraph = ruleResult.getAssemblyGraph();

          if( finalFlowElementGraph.vertexSet().isEmpty() )
            throw new PlannerException( "final assembly graph is empty: " + registryName );

          break;

        case Step:

          Map<ElementGraph, List<? extends ElementGraph>> assemblyToSteps = ruleResult.getAssemblyToStepGraphMap();

          if( assemblyToSteps.isEmpty() )
            throw new PlannerException( "no steps partitioned: " + registryName );

          for( ElementGraph assembly : assemblyToSteps.keySet() )
            {
            List<? extends ElementGraph> steps = assemblyToSteps.get( assembly );

            if( steps.isEmpty() )
              throw new PlannerException( "no steps partitioned from assembly: " + registryName, assembly );

            Set<ElementGraph> stepSet = new HashSet<>( steps.size() );

            for( ElementGraph step : steps )
              {
              if( !stepSet.add( step ) )
                throw new PlannerException( "found duplicate step in flow: " + registryName, step );
              }

            Set<FlowElement> elements = createIdentitySet();

            for( ElementGraph step : steps )
              elements.addAll( step.vertexSet() );

            Set<FlowElement> missing = differenceIdentity( assembly.vertexSet(), elements );

            if( !missing.isEmpty() )
              {
              String message = "union of steps have " + missing.size() + " fewer elements than parent assembly: " + registryName + ", missing: [" + join( missing, ", " ) + "]";
              throw new PlannerException( message, assembly );
              }
            }

          break;

        case Node:

          Map<ElementGraph, List<? extends ElementGraph>> stepToNodes = ruleResult.getStepToNodeGraphMap();

          if( stepToNodes.isEmpty() )
            throw new PlannerException( "no nodes partitioned: " + registryName );

          for( ElementGraph step : stepToNodes.keySet() )
            {
            List<? extends ElementGraph> nodes = stepToNodes.get( step );

            if( nodes.isEmpty() )
              throw new PlannerException( "no nodes partitioned from step: " + registryName, step );

            Set<ElementGraph> nodesSet = new HashSet<>( nodes.size() );

            for( ElementGraph node : nodes )
              {
              if( !nodesSet.add( node ) )
                throw new PlannerException( "found duplicate node in step: " + registryName, node );
              }

            Set<FlowElement> elements = createIdentitySet();

            for( ElementGraph node : nodes )
              elements.addAll( node.vertexSet() );

            Set<FlowElement> missing = differenceIdentity( step.vertexSet(), elements );

            if( !missing.isEmpty() )
              {
              String message = "union of nodes have " + missing.size() + " fewer elements than parent step: " + registryName + ", missing: [" + join( missing, ", " ) + "]";
              throw new PlannerException( message, step );
              }
            }

          break;

        case Pipeline:

          // all nodes are partitioned into pipelines, but if partitioned all elements should be represented
          Map<ElementGraph, List<? extends ElementGraph>> nodeToPipeline = ruleResult.getNodeToPipelineGraphMap();

          if( nodeToPipeline.isEmpty() )
            throw new PlannerException( "no pipelines partitioned: " + registryName );

          for( ElementGraph node : nodeToPipeline.keySet() )
            {
            List<? extends ElementGraph> pipelines = nodeToPipeline.get( node );

            if( pipelines.isEmpty() )
              throw new PlannerException( "no pipelines partitioned from node: " + registryName, node );

            Set<ElementGraph> pipelineSet = new HashSet<>( pipelines.size() );

            for( ElementGraph pipeline : pipelines )
              {
              if( !pipelineSet.add( pipeline ) )
                throw new PlannerException( "found duplicate pipeline in node: " + registryName, pipeline );
              }

            Set<FlowElement> elements = createIdentitySet();

            for( ElementGraph pipeline : pipelines )
              elements.addAll( pipeline.vertexSet() );

            Set<FlowElement> missing = differenceIdentity( node.vertexSet(), elements );

            if( !missing.isEmpty() )
              {
              String message = "union of pipelines have " + missing.size() + " fewer elements than parent node: " + registryName + ", missing: [" + join( missing, ", " ) + "]";
              throw new PlannerException( message, node );
              }
            }

          break;
        }
      }
    }

  protected PlannerException handleExceptionDuringPlanning( FlowDef flowDef, Exception exception, FlowElementGraph flowElementGraph )
    {
    if( exception instanceof PlannerException )
      {
      if( ( (PlannerException) exception ).elementGraph == null )
        ( (PlannerException) exception ).elementGraph = flowElementGraph;

      return (PlannerException) exception;
      }
    else if( exception instanceof ElementGraphException )
      {
      Throwable cause = exception.getCause();

      if( cause == null )
        cause = exception;

      // captures pipegraph for debugging
      // forward message in case cause or trace is lost
      String message = String.format( "[%s] could not build flow from assembly", Util.truncate( flowDef.getName(), 25 ) );

      if( cause.getMessage() != null )
        message = String.format( "%s: [%s]", message, cause.getMessage() );

      if( cause instanceof OperatorException )
        return new PlannerException( message, cause, flowElementGraph );

      if( cause instanceof TapException )
        return new PlannerException( message, cause, flowElementGraph );

      return new PlannerException( ( (ElementGraphException) exception ).getPipe(), message, cause, flowElementGraph );
      }
    else
      {
      // captures pipegraph for debugging
      // forward message in case cause or trace is lost
      String message = String.format( "[%s] could not build flow from assembly", Util.truncate( flowDef.getName(), 25 ) );

      if( exception.getMessage() != null )
        message = String.format( "%s: [%s]", message, exception.getMessage() );

      return new PlannerException( message, exception, flowElementGraph );
      }
    }

  public class TempTapElementFactory extends IntermediateTapElementFactory
    {
    private String defaultDecoratorClassName;

    public TempTapElementFactory()
      {
      }

    public TempTapElementFactory( String defaultDecoratorClassName )
      {
      this.defaultDecoratorClassName = defaultDecoratorClassName;
      }

    @Override
    public FlowElement create( ElementGraph graph, FlowElement flowElement )
      {
      if( flowElement instanceof Pipe )
        return makeTempTap( (FlowElementGraph) graph, (Pipe) flowElement, defaultDecoratorClassName );

      if( flowElement instanceof Tap )
        return decorateTap( null, (Tap) flowElement, FlowConnectorProps.TEMPORARY_TAP_DECORATOR_CLASS, defaultDecoratorClassName );

      throw new IllegalStateException( "unknown flow element type: " + flowElement );
      }
    }

  private Tap makeTempTap( FlowElementGraph graph, Pipe pipe, String defaultDecoratorClassName )
    {
    Tap checkpointTap = graph.getCheckpointsMap().get( pipe.getName() );

    if( checkpointTap != null )
      {
      LOG.info( "found checkpoint: {}, using tap: {}", pipe.getName(), checkpointTap );
      checkpointTap = decorateTap( pipe, checkpointTap, FlowConnectorProps.CHECKPOINT_TAP_DECORATOR_CLASS, null );
      }

    if( checkpointTap == null )
      {
      // only restart from a checkpoint pipe or checkpoint tap below
      if( pipe instanceof Checkpoint )
        {
        checkpointTap = makeTempTap( checkpointTapRootPath, pipe.getName() );
        checkpointTap = decorateTap( pipe, checkpointTap, FlowConnectorProps.CHECKPOINT_TAP_DECORATOR_CLASS, null );
        // mark as an anonymous checkpoint
        checkpointTap.getConfigDef().setProperty( ConfigDef.Mode.DEFAULT, "cascading.checkpoint", "true" );
        }
      else
        {
        checkpointTap = makeTempTap( pipe.getName() );
        }
      }

    return decorateTap( pipe, checkpointTap, FlowConnectorProps.TEMPORARY_TAP_DECORATOR_CLASS, defaultDecoratorClassName );
    }

  private Tap decorateTap( Pipe pipe, Tap tempTap, String decoratorClassProp, String defaultDecoratorClassName )
    {
    String decoratorClassName = PropertyUtil.getProperty( defaultProperties, pipe, decoratorClassProp );

    if( Util.isEmpty( decoratorClassName ) )
      decoratorClassName = defaultDecoratorClassName;

    if( Util.isEmpty( decoratorClassName ) )
      return tempTap;

    LOG.info( "found decorator property: {}, with value: {}, wrapping tap: {}", decoratorClassProp, decoratorClassName, tempTap );

    tempTap = Util.newInstance( decoratorClassName, tempTap );

    return tempTap;
    }

  protected Tap makeTempTap( String name )
    {
    return makeTempTap( null, name );
    }

  protected DebugLevel getDebugLevel( FlowDef flowDef )
    {
    return flowDef.getDebugLevel() == null ? this.defaultDebugLevel : flowDef.getDebugLevel();
    }

  protected AssertionLevel getAssertionLevel( FlowDef flowDef )
    {
    return flowDef.getAssertionLevel() == null ? this.defaultAssertionLevel : flowDef.getAssertionLevel();
    }

  protected abstract Tap makeTempTap( String prefix, String name );

  private Set<ProcessLevel> getReverseOrderedProcessLevels( RuleResult ruleResult )
    {
    Set<ProcessLevel> ordered = new TreeSet<>( Collections.reverseOrder() );

    ordered.addAll( ruleResult.getRegistry().getProcessLevels() );

    return ordered;
    }
  }
