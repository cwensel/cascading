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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.AssemblyPlanner;
import cascading.flow.BaseFlow;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.FlowElement;
import cascading.flow.FlowStep;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.iso.transformer.ElementFactory;
import cascading.flow.planner.rule.PlanPhase;
import cascading.flow.planner.rule.RuleExec;
import cascading.flow.planner.rule.RuleRegistry;
import cascading.flow.planner.rule.RuleResult;
import cascading.operation.AssertionLevel;
import cascading.operation.DebugLevel;
import cascading.pipe.Checkpoint;
import cascading.pipe.OperatorException;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.property.AppProps;
import cascading.property.PropertyUtil;
import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.Fields;
import cascading.util.Util;
import cascading.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  public Map<Object, Object> getDefaultProperties()
    {
    return defaultProperties;
    }

  public abstract Config getDefaultConfig();

  public abstract PlatformInfo getPlatformInfo();

  public void initialize( FlowConnector flowConnector, Map<Object, Object> properties )
    {
    this.defaultProperties = properties;
    this.defaultAssertionLevel = getAssertionLevel( properties );
    this.defaultDebugLevel = getDebugLevel( properties );
    }

  public F buildFlow( FlowDef flowDef )
    {
    FlowElementGraph flowElementGraph = null;

    try
      {
      verifyAllTaps( flowDef );

      F flow = createFlow( flowDef );
      String nameOrID = getNameOrID( flow );
      String transformPath = getPlanTransformTracePath();

      if( transformPath != null )
        transformPath = FileSystems.getDefault().getPath( transformPath, nameOrID ).toString();

      Pipe[] tails = resolveTails( flowDef, flow );

      verifyAssembly( flowDef, tails );

      RuleRegistry ruleRegistry = getRuleRegistry( flowDef );

      configRuleRegistry( ruleRegistry );

      RuleExec ruleExec = new RuleExec( ruleRegistry );

      flowElementGraph = createFlowElementGraph( flowDef, tails );

      writeTracePlan( nameOrID, "0-initial-flow-element-graph", flowElementGraph );

      ruleExec.enableTransformTracing( transformPath );

      PlannerContext plannerContext = new PlannerContext( ruleRegistry, this, flowDef, flow, transformPath );

      RuleResult ruleResult = ruleExec.exec( plannerContext, flowElementGraph );

      writeTracePlan( nameOrID, "1-completed-flow-element-graph", ruleResult.getAssemblyResult( PlanPhase.PostResolveAssembly ) );

      writeStats( plannerContext, nameOrID, ruleResult );

      FlowElementGraph finalFlowElementGraph = ruleResult.getAssemblyResult( PlanPhase.PostResolveAssembly );

      FlowStepGraph flowStepGraph = new FlowStepGraph( transformPath, this, finalFlowElementGraph, ruleResult.getNodeSubGraphResults(), ruleResult.getNodePipelineGraphResults() );

      writeTracePlan( nameOrID, "2-completed-flow-step-graph", flowStepGraph );

      flow.initialize( finalFlowElementGraph, flowStepGraph );

      return flow;
      }
    catch( Exception exception )
      {
      throw handleExceptionDuringPlanning( exception, flowElementGraph );
      }
    }

  private String getNameOrID( F flow )
    {
    if( flow.getName() != null )
      return flow.getName();

    return flow.getID().substring( 0, 6 );
    }

  protected void configRuleRegistry( RuleRegistry ruleRegistry )
    {

    }

  protected abstract FlowStep<Config> createFlowStep( int numSteps, int ordinal, ElementGraph stepElementGraph, FlowNodeGraph flowNodeGraph );

  protected abstract RuleRegistry getRuleRegistry( FlowDef flowDef );

  protected abstract F createFlow( FlowDef flowDef );

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
//            LOG.warn( "duplicate tail name found: '{}'", tailName );
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
//          LOG.warn( "duplicate tail name found: '{}'", tailName );
          throw new PlannerException( pipe, "duplicate tail name found: " + tailName );

        tailNames.add( tailName );
        tails.add( pipe );
        }
      }

//    Set<String> allTailNames = new HashSet<String>( tailNames );
    tailNames.removeAll( flowDef.getSinks().keySet() );
    Set<String> remainingSinks = new HashSet<String>( flowDef.getSinks().keySet() );
    remainingSinks.removeAll( tailNames );

    if( tailNames.size() != 0 )
      throw new PlannerException( "not all tail pipes bound to sink taps, remaining tail pipe names: [" + Util.join( Util.quote( tailNames, "'" ), ", " ) + "], remaining sink tap names: [" + Util.join( Util.quote( remainingSinks, "'" ), ", " ) + "]" );

    // unlike heads, pipes can input to another pipe and simultaneously be a sink
    // so there is no way to know all the intentional tails, so they aren't listed below in the exception
    remainingSinks = new HashSet<String>( flowDef.getSinks().keySet() );
    remainingSinks.removeAll( asList( Pipe.names( flowTails ) ) );

    if( remainingSinks.size() != 0 )
      throw new PlannerException( "not all sink taps bound to tail pipes, remaining sink tap names: [" + Util.join( Util.quote( remainingSinks, "'" ), ", " ) + "]" );

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
//          throw new PlannerException( pipe, "duplicate head name found: " + headName );

        headNames.add( headName );
        heads.add( head );
        }
      }

    Set<String> allHeadNames = new HashSet<String>( headNames );
    headNames.removeAll( flowDef.getSources().keySet() );
    Set<String> remainingSources = new HashSet<String>( flowDef.getSources().keySet() );
    remainingSources.removeAll( headNames );

    if( headNames.size() != 0 )
      throw new PlannerException( "not all head pipes bound to source taps, remaining head pipe names: [" + Util.join( Util.quote( headNames, "'" ), ", " ) + "], remaining source tap names: [" + Util.join( Util.quote( remainingSources, "'" ), ", " ) + "]" );

    remainingSources = new HashSet<String>( flowDef.getSources().keySet() );
    remainingSources.removeAll( allHeadNames );

    if( remainingSources.size() != 0 )
      throw new PlannerException( "not all source taps bound to head pipes, remaining source tap names: [" + Util.join( Util.quote( remainingSources, "'" ), ", " ) + "], remaining head pipe names: [" + Util.join( Util.quote( headNames, "'" ), ", " ) + "]" );

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
        throw new PlannerException( "checkpoint name not found in assembly: '" + name + "'" );

      Set<Pipe> pipes = new HashSet<Pipe>( asList( Pipe.named( name, flowTails ) ) );

      int count = 0;

      for( Pipe pipe : pipes )
        {
        if( pipe instanceof Checkpoint )
          count++;
        }

      if( count == 0 )
        throw new PlannerException( "no checkpoint with name found in assembly: '" + name + "'" );

      if( count > 1 )
        throw new PlannerException( "more than one checkpoint with name found in assembly: '" + name + "'" );
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

  protected PlannerException handleExceptionDuringPlanning( Exception exception, FlowElementGraph flowElementGraph )
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
      String message = String.format( "could not build flow from assembly: [%s]", cause.getMessage() );

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
      String message = String.format( "could not build flow from assembly: [%s]", exception.getMessage() );
      return new PlannerException( message, exception, flowElementGraph );
      }
    }

  public class TempTapElementFactory implements ElementFactory
    {
    @Override
    public FlowElement create( ElementGraph graph, FlowElement flowElement )
      {
      return makeTempTap( (FlowElementGraph) graph, (Pipe) flowElement );
      }
    }

  private Tap makeTempTap( FlowElementGraph graph, Pipe pipe )
    {
    Tap checkpointTap = graph.getCheckpointsMap().get( pipe.getName() );

    if( checkpointTap != null )
      LOG.info( "found checkpoint: {}, using tap: {}", pipe.getName(), checkpointTap );

    if( checkpointTap == null )
      {
      // only restart from a checkpoint pipe or checkpoint tap below
      if( pipe instanceof Checkpoint )
        checkpointTap = makeTempTap( checkpointTapRootPath, pipe.getName() );
      else
        checkpointTap = makeTempTap( pipe.getName() );
      }

    return checkpointTap;
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

  protected String getPlanTracePath()
    {
    return System.getProperty( FlowPlanner.TRACE_PLAN_PATH );
    }

  protected String getPlanTransformTracePath()
    {
    return System.getProperty( FlowPlanner.TRACE_PLAN_TRANSFORM_PATH );
    }

  protected String getPlanStatsPath()
    {
    return System.getProperty( FlowPlanner.TRACE_STATS_PATH );
    }

  protected void writeTracePlan( String flowName, String fileName, FlowElementGraph flowElementGraph )
    {
    String path = getPlanTracePath();

    if( path == null )
      return;

    Path filePath = FileSystems.getDefault().getPath( path, flowName, String.format( "%s.dot", fileName ) );
    File file = filePath.toFile();

    LOG.info( "writing trace element plan: {}", file );

    String filename = file.toString();

    flowElementGraph.writeDOT( filename );
    }

  protected void writeTracePlan( String flowName, String fileName, FlowStepGraph stepGraph )
    {
    String path = getPlanTracePath();

    if( path == null )
      return;

    Path filePath = FileSystems.getDefault().getPath( path, flowName, String.format( "%s.dot", fileName ) );
    File file = filePath.toFile();

    LOG.info( "writing trace step plan: {}", file );

    stepGraph.writeDOT( file.toString() );
    }

  private void writeStats( PlannerContext plannerContext, String flowName, RuleResult ruleResult )
    {
    if( getPlanStatsPath() == null )
      return;

    Path path = FileSystems.getDefault().getPath( getPlanStatsPath(), flowName, "planner-stats.txt" );
    File file = path.toFile();

    LOG.info( "writing planner stats to: {}", file );

    file.getParentFile().mkdirs();

    try (PrintWriter writer = new PrintWriter( file ))
      {
      Flow flow = plannerContext.getFlow();

      writer.format( "cascading version: %s, build: %s\n", emptyOrValue( Version.getReleaseFull() ), emptyOrValue( Version.getReleaseBuild() ) );
      writer.format( "application id: %s\n", emptyOrValue( AppProps.getApplicationID( flow.getConfigAsProperties() ) ) );
      writer.format( "application name: %s\n", emptyOrValue( AppProps.getApplicationName( flow.getConfigAsProperties() ) ) );
      writer.format( "application version: %s\n", emptyOrValue( AppProps.getApplicationVersion( flow.getConfigAsProperties() ) ) );
      writer.format( "platform: %s\n", emptyOrValue( flow.getPlatformInfo() ) );
      writer.format( "frameworks: %s\n", emptyOrValue( AppProps.getApplicationFrameworks( flow.getConfigAsProperties() ) ) );

      writer.println();

      ruleResult.writeStats( writer );
      }
    catch( IOException exception )
      {
      LOG.error( "could not write stats", exception );
      }
    }

  private static String emptyOrValue( Object value )
    {
    if( value == null )
      return "";

    if( Util.isEmpty( value.toString() ) )
      return "";

    return value.toString();
    }
  }
