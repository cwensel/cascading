/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.Flow;
import cascading.flow.FlowElement;
import cascading.flow.FlowException;
import cascading.flow.FlowNode;
import cascading.flow.FlowProcess;
import cascading.flow.FlowStep;
import cascading.flow.FlowStepListener;
import cascading.flow.planner.graph.AnnotatedGraph;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.graph.ElementGraphs;
import cascading.flow.planner.process.FlowNodeGraph;
import cascading.flow.stream.annotations.StreamMode;
import cascading.management.CascadingServices;
import cascading.management.state.ClientState;
import cascading.operation.Operation;
import cascading.pipe.Group;
import cascading.pipe.Operator;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.property.ConfigDef;
import cascading.stats.FlowStepStats;
import cascading.tap.Tap;
import cascading.util.EnumMultiMap;
import cascading.util.ProcessLogger;
import cascading.util.Util;

import static cascading.flow.planner.graph.ElementGraphs.findAllGroups;

/**
 * Class FlowStep is an internal representation of a given Job to be executed on a remote cluster. During
 * planning, pipe assemblies are broken down into "steps" and encapsulated in this class.
 * <p/>
 * FlowSteps are submitted in order of dependency. If two or more steps do not share the same dependencies and all
 * can be scheduled simultaneously, the {@link #getSubmitPriority()} value determines the order in which
 * all steps will be submitted for execution. The default submit priority is 5.
 * <p/>
 * This class is for internal use, there are no stable public methods.
 */
public abstract class BaseFlowStep<Config> implements FlowStep<Config>, ProcessLogger, Serializable
  {
  /** Field flow */
  private transient Flow<Config> flow;
  /** Field flowName */
  private String flowName;
  /** Field flowID */
  private String flowID;

  private transient Config flowStepConf;

  /** Field submitPriority */
  private int submitPriority = 5;

  /** Field name */
  String name;
  private String id;
  private int ordinal;
  private Map<String, String> processAnnotations;

  /** Field step listeners */
  private List<SafeFlowStepListener> listeners;

  /** Field graph */
  private final ElementGraph elementGraph;

  private FlowNodeGraph flowNodeGraph;

  /** Field sources */
  protected final Map<Tap, Set<String>> sources = new HashMap<>(); // all sources
  /** Field sink */
  protected final Map<Tap, Set<String>> sinks = new HashMap<>(); // all sinks

  /** Field mapperTraps */
  private final Map<String, Tap> traps = new HashMap<>();

  /** Field tempSink */
  protected Tap tempSink; // used if we need to bypass the filesystem

  /** Field groups */
  private final List<Group> groups = new ArrayList<Group>();

  protected transient FlowStepStats flowStepStats;

  private transient FlowStepJob<Config> flowStepJob;

  /** optional metadata about the FlowStep */
  private Map<String, String> flowStepDescriptor = Collections.emptyMap();

  protected BaseFlowStep( String name, int ordinal )
    {
    this( name, ordinal, null );
    }

  protected BaseFlowStep( String name, int ordinal, Map<String, String> flowStepDescriptor )
    {
    this( name, ordinal, null, flowStepDescriptor );
    }

  protected BaseFlowStep( String name, int ordinal, FlowNodeGraph flowNodeGraph, Map<String, String> flowStepDescriptor )
    {
    this.id = Util.createUniqueIDWhichStartsWithAChar(); // timeline server cannot filter strings that start with a number
    setName( name );
    this.ordinal = ordinal;

    this.elementGraph = null;
    this.flowNodeGraph = flowNodeGraph;

    if( flowStepDescriptor != null )
      this.flowStepDescriptor = flowStepDescriptor;
    }

  protected BaseFlowStep( ElementGraph elementStepGraph, FlowNodeGraph flowNodeGraph )
    {
    this( elementStepGraph, flowNodeGraph, null );
    }

  protected BaseFlowStep( ElementGraph elementStepGraph, FlowNodeGraph flowNodeGraph, Map<String, String> flowStepDescriptor )
    {
    this.id = Util.createUniqueIDWhichStartsWithAChar(); // timeline server cannot filter strings that start with a number
    this.elementGraph = elementStepGraph;
    this.flowNodeGraph = flowNodeGraph; // TODO: verify no missing elements in the union of the node graphs

    if( flowStepDescriptor != null )
      this.flowStepDescriptor = flowStepDescriptor;

    configure();
    }

  protected void configure()
    {
    // todo: remove once FlowMapper/FlowReducer aren't reliant
    ElementGraphs.addSources( this, elementGraph, flowNodeGraph.getSourceTaps() );
    ElementGraphs.addSinks( this, elementGraph, flowNodeGraph.getSinkTaps() );

    addGroups( findAllGroups( elementGraph ) );

    traps.putAll( flowNodeGraph.getTrapsMap() );
    }

  @Override
  public String getID()
    {
    return id;
    }

  public void setOrdinal( int ordinal )
    {
    this.ordinal = ordinal;
    }

  @Override
  public int getOrdinal()
    {
    return ordinal;
    }

  @Override
  public String getName()
    {
    return name;
    }

  public void setName( String name )
    {
    if( name == null || name.isEmpty() )
      throw new IllegalArgumentException( "step name may not be null or empty" );

    this.name = name;
    }

  @Override
  public Map<String, String> getFlowStepDescriptor()
    {
    return Collections.unmodifiableMap( flowStepDescriptor );
    }

  @Override
  public Map<String, String> getProcessAnnotations()
    {
    if( processAnnotations == null )
      return Collections.emptyMap();

    return Collections.unmodifiableMap( processAnnotations );
    }

  @Override
  public void addProcessAnnotation( Enum annotation )
    {
    if( annotation == null )
      return;

    addProcessAnnotation( annotation.getDeclaringClass().getName(), annotation.name() );
    }

  @Override
  public void addProcessAnnotation( String key, String value )
    {
    if( processAnnotations == null )
      processAnnotations = new HashMap<>();

    processAnnotations.put( key, value );
    }

  public void setFlow( Flow<Config> flow )
    {
    this.flow = flow;
    this.flowID = flow.getID();
    this.flowName = flow.getName();
    }

  @Override
  public Flow<Config> getFlow()
    {
    return flow;
    }

  @Override
  public String getFlowID()
    {
    return flowID;
    }

  @Override
  public String getFlowName()
    {
    return flowName;
    }

  protected void setFlowName( String flowName )
    {
    this.flowName = flowName;
    }

  @Override
  public Config getConfig()
    {
    return flowStepConf;
    }

  @Override
  public Map<Object, Object> getConfigAsProperties()
    {
    return Collections.emptyMap();
    }

  /**
   * Set the initialized flowStepConf Config instance
   *
   * @param flowStepConf of type Config
   */
  protected void setConfig( Config flowStepConf )
    {
    this.flowStepConf = flowStepConf;
    }

  @Override
  public String getStepDisplayName()
    {
    return getStepDisplayName( Util.ID_LENGTH );
    }

  protected String getStepDisplayName( int idLength )
    {
    if( idLength < 0 || idLength > Util.ID_LENGTH )
      idLength = Util.ID_LENGTH;

    if( idLength == 0 )
      return String.format( "%s/%s", getFlowName(), getName() );

    String flowID = getFlowID().substring( 0, idLength );
    String stepID = getID().substring( 0, idLength );

    return String.format( "[%s/%s] %s/%s", flowID, stepID, getFlowName(), getName() );
    }

  protected String getNodeDisplayName( FlowNode flowNode, int idLength )
    {
    if( idLength > Util.ID_LENGTH )
      idLength = Util.ID_LENGTH;

    String flowID = getFlowID().substring( 0, idLength );
    String stepID = getID().substring( 0, idLength );
    String nodeID = flowNode.getID().substring( 0, idLength );

    return String.format( "[%s/%s/%s] %s/%s", flowID, stepID, nodeID, getFlowName(), getName() );
    }

  @Override
  public int getSubmitPriority()
    {
    return submitPriority;
    }

  @Override
  public void setSubmitPriority( int submitPriority )
    {
    if( submitPriority < 1 || submitPriority > 10 )
      throw new IllegalArgumentException( "submitPriority must be between 1 and 10 inclusive, was: " + submitPriority );

    this.submitPriority = submitPriority;
    }

  @Override
  public void setFlowStepStats( FlowStepStats flowStepStats )
    {
    this.flowStepStats = flowStepStats;
    }

  @Override
  public FlowStepStats getFlowStepStats()
    {
    return flowStepStats;
    }

  @Override
  public ElementGraph getElementGraph()
    {
    return elementGraph;
    }

  protected EnumMultiMap getAnnotations()
    {
    return ( (AnnotatedGraph) elementGraph ).getAnnotations();
    }

  @Override
  public FlowNodeGraph getFlowNodeGraph()
    {
    return flowNodeGraph;
    }

  @Override
  public int getNumFlowNodes()
    {
    return flowNodeGraph.vertexSet().size();
    }

  public Set<FlowElement> getSourceElements()
    {
    return ElementGraphs.findSources( getElementGraph(), FlowElement.class );
    }

  public Set<FlowElement> getSinkElements()
    {
    return ElementGraphs.findSinks( getElementGraph(), FlowElement.class );
    }

  @Override
  public Group getGroup()
    {
    if( groups.isEmpty() )
      return null;

    if( groups.size() > 1 )
      throw new IllegalStateException( "more than one group" );

    return groups.get( 0 );
    }

  @Override
  public Collection<Group> getGroups()
    {
    return groups;
    }

  public void addGroups( Collection<Group> groups )
    {
    for( Group group : groups )
      addGroup( group );
    }

  public void addGroup( Group group )
    {
    if( !groups.contains( group ) )
      groups.add( group );
    }

  public Set<Tap> getAllAccumulatedSources()
    {
    return Util.narrowSet( Tap.class, getFlowNodeGraph().getFlowElementsFor( StreamMode.Accumulated ) );
    }

  public void addSource( String name, Tap source )
    {
    if( !sources.containsKey( source ) )
      sources.put( source, new HashSet<String>() );

    sources.get( source ).add( name );
    }

  public void addSink( String name, Tap sink )
    {
    if( !sinks.containsKey( sink ) )
      sinks.put( sink, new HashSet<String>() );

    sinks.get( sink ).add( name );
    }

  @Override
  public Set<Tap> getSourceTaps()
    {
    return Collections.unmodifiableSet( new HashSet<Tap>( sources.keySet() ) );
    }

  @Override
  public Set<Tap> getSinkTaps()
    {
    return Collections.unmodifiableSet( new HashSet<Tap>( sinks.keySet() ) );
    }

  @Override
  public Tap getSink()
    {
    if( sinks.size() != 1 )
      throw new IllegalStateException( "more than one sink" );

    return sinks.keySet().iterator().next();
    }

  @Override
  public Set<String> getSourceName( Tap source )
    {
    return Collections.unmodifiableSet( sources.get( source ) );
    }

  @Override
  public Set<String> getSinkName( Tap sink )
    {
    return Collections.unmodifiableSet( sinks.get( sink ) );
    }

  @Override
  public Tap getSourceWith( String identifier )
    {
    for( Tap tap : sources.keySet() )
      {
      if( tap.getIdentifier().equalsIgnoreCase( identifier ) )
        return tap;
      }

    return null;
    }

  @Override
  public Tap getSinkWith( String identifier )
    {
    for( Tap tap : sinks.keySet() )
      {
      if( tap.getIdentifier().equalsIgnoreCase( identifier ) )
        return tap;
      }

    return null;
    }

  @Override
  public Map<String, Tap> getTrapMap()
    {
    return traps;
    }

  @Override
  public Set<Tap> getTraps()
    {
    return Collections.unmodifiableSet( new HashSet<Tap>( traps.values() ) );
    }

  public Tap getTrap( String name )
    {
    return getTrapMap().get( name );
    }

  boolean allSourcesExist() throws IOException
    {
    for( Tap tap : sources.keySet() )
      {
      if( !tap.resourceExists( getConfig() ) )
        return false;
      }

    return true;
    }

  boolean areSourcesNewer( long sinkModified ) throws IOException
    {
    Config config = getConfig();
    Iterator<Tap> values = sources.keySet().iterator();

    long sourceModified = 0;

    try
      {
      sourceModified = Util.getSourceModified( config, values, sinkModified );

      if( sinkModified < sourceModified )
        return true;

      return false;
      }
    finally
      {
      if( isInfoEnabled() )
        logInfo( "source modification date at: " + new Date( sourceModified ) ); // not oldest, we didnt check them all
      }
    }

  long getSinkModified() throws IOException
    {
    long sinkModified = Util.getSinkModified( getConfig(), sinks.keySet() );

    if( isInfoEnabled() )
      {
      if( sinkModified == -1L )
        logInfo( "at least one sink is marked for delete" );
      if( sinkModified == 0L )
        logInfo( "at least one sink does not exist" );
      else
        logInfo( "sink oldest modified date: " + new Date( sinkModified ) );
      }

    return sinkModified;
    }

  protected Throwable prepareResources()
    {
    Throwable throwable = prepareResources( getSourceTaps(), false );

    if( throwable == null )
      throwable = prepareResources( getSinkTaps(), true );

    if( throwable == null )
      throwable = prepareResources( getTraps(), true );

    return throwable;
    }

  private Throwable prepareResources( Collection<Tap> taps, boolean forWrite )
    {
    Throwable throwable = null;

    for( Tap tap : taps )
      {
      throwable = prepareResource( tap, forWrite );

      if( throwable != null )
        break;
      }

    return throwable;
    }

  private Throwable prepareResource( Tap tap, boolean forWrite )
    {
    Throwable throwable = null;

    try
      {
      boolean result;

      if( forWrite )
        result = tap.prepareResourceForWrite( getConfig() );
      else
        result = tap.prepareResourceForRead( getConfig() );

      if( !result )
        {
        String message = String.format( "unable to prepare tap for %s: %s", forWrite ? "write" : "read", tap.getFullIdentifier( getConfig() ) );

        logError( message );

        throwable = new FlowException( message );
        }
      }
    catch( Throwable exception )
      {
      String message = String.format( "unable to prepare tap for %s: %s", forWrite ? "write" : "read", tap.getFullIdentifier( getConfig() ) );

      logError( message, exception );

      throwable = new FlowException( message, exception );
      }

    return throwable;
    }

  protected Throwable commitSinks()
    {
    Throwable throwable = null;

    for( Tap tap : sinks.keySet() )
      {
      if( throwable != null )
        rollbackResource( tap );
      else
        throwable = commitResource( tap );
      }

    return throwable;
    }

  private Throwable commitResource( Tap tap )
    {
    Throwable throwable = null;

    try
      {
      if( !tap.commitResource( getConfig() ) )
        {
        String message = "unable to commit sink: " + tap.getFullIdentifier( getConfig() );

        logError( message );

        throwable = new FlowException( message );
        }
      }
    catch( Throwable exception )
      {
      String message = "unable to commit sink: " + tap.getFullIdentifier( getConfig() );

      logError( message, exception );

      throwable = new FlowException( message, exception );
      }

    return throwable;
    }

  private Throwable rollbackResource( Tap tap )
    {
    Throwable throwable = null;

    try
      {
      if( !tap.rollbackResource( getConfig() ) )
        {
        String message = "unable to rollback sink: " + tap.getFullIdentifier( getConfig() );

        logError( message );

        throwable = new FlowException( message );
        }
      }
    catch( Throwable exception )
      {
      String message = "unable to rollback sink: " + tap.getFullIdentifier( getConfig() );

      logError( message, exception );

      throwable = new FlowException( message, exception );
      }

    return throwable;
    }

  protected Throwable rollbackSinks()
    {
    Throwable throwable = null;

    for( Tap tap : sinks.keySet() )
      {
      if( throwable != null )
        rollbackResource( tap );
      else
        throwable = rollbackResource( tap );
      }

    return throwable;
    }

  /**
   * Public for testing.
   *
   * @param flowProcess
   * @param parentConfig
   * @return
   */
  public abstract Config createInitializedConfig( FlowProcess<Config> flowProcess, Config parentConfig );

  /**
   * Method getPreviousScopes returns the previous Scope instances. If the flowElement is a Group (specifically a CoGroup),
   * there will be more than one instance.
   *
   * @param flowElement of type FlowElement
   * @return Set<Scope>
   */
  public Set<Scope> getPreviousScopes( FlowElement flowElement )
    {
    return getElementGraph().incomingEdgesOf( flowElement );
    }

  /**
   * Method getNextScope returns the next Scope instance in the graph. There will always only be one next.
   *
   * @param flowElement of type FlowElement
   * @return Scope
   */
  public Scope getNextScope( FlowElement flowElement )
    {
    Set<Scope> set = getElementGraph().outgoingEdgesOf( flowElement );

    if( set.size() != 1 )
      throw new IllegalStateException( "should only be one scope after current flow element: " + flowElement + " found: " + set.size() );

    return set.iterator().next();
    }

  public FlowElement getNextFlowElement( Scope scope )
    {
    return getElementGraph().getEdgeTarget( scope );
    }

  public Collection<Operation> getAllOperations()
    {
    Set<FlowElement> vertices = getElementGraph().vertexSet();
    List<Operation> operations = new ArrayList<Operation>(); // operations impl equals, so two instance may be the same

    for( FlowElement vertex : vertices )
      {
      if( vertex instanceof Operator )
        operations.add( ( (Operator) vertex ).getOperation() );
      }

    return operations;
    }

  @Override
  public boolean containsPipeNamed( String pipeName )
    {
    Set<FlowElement> vertices = getElementGraph().vertexSet();

    for( FlowElement vertex : vertices )
      {
      if( vertex instanceof Pipe && ( (Pipe) vertex ).getName().equals( pipeName ) )
        return true;
      }

    return false;
    }

  public void clean()
    {
    // use step config by default
    clean( getConfig() );
    }

  public abstract void clean( Config config );

  List<SafeFlowStepListener> getListeners()
    {
    if( listeners == null )
      listeners = new LinkedList<SafeFlowStepListener>();

    return listeners;
    }

  @Override
  public boolean hasListeners()
    {
    return listeners != null && !listeners.isEmpty();
    }

  @Override
  public void addListener( FlowStepListener flowStepListener )
    {
    getListeners().add( new SafeFlowStepListener( flowStepListener ) );
    }

  @Override
  public boolean removeListener( FlowStepListener flowStepListener )
    {
    return getListeners().remove( new SafeFlowStepListener( flowStepListener ) );
    }

  protected void fireOnCompleted()
    {
    if( hasListeners() )
      {
      if( isDebugEnabled() )
        logDebug( "firing onCompleted event: " + getListeners().size() );

      for( Object flowStepListener : getListeners() )
        ( (FlowStepListener) flowStepListener ).onStepCompleted( this );
      }
    }

  protected void fireOnThrowable( Throwable throwable )
    {
    if( hasListeners() )
      {
      if( isDebugEnabled() )
        logDebug( "firing onThrowable event: " + getListeners().size() );

      for( Object flowStepListener : getListeners() )
        ( (FlowStepListener) flowStepListener ).onStepThrowable( this, throwable );
      }
    }

  protected void fireOnStopping()
    {
    if( hasListeners() )
      {
      if( isDebugEnabled() )
        logDebug( "firing onStopping event: " + getListeners() );

      for( Object flowStepListener : getListeners() )
        ( (FlowStepListener) flowStepListener ).onStepStopping( this );
      }
    }

  protected void fireOnStarting()
    {
    if( hasListeners() )
      {
      if( isDebugEnabled() )
        logDebug( "firing onStarting event: " + getListeners().size() );

      for( Object flowStepListener : getListeners() )
        ( (FlowStepListener) flowStepListener ).onStepStarting( this );
      }
    }

  protected void fireOnRunning()
    {
    if( hasListeners() )
      {
      if( isDebugEnabled() )
        logDebug( "firing onRunning event: " + getListeners().size() );

      for( Object flowStepListener : getListeners() )
        ( (FlowStepListener) flowStepListener ).onStepRunning( this );
      }
    }

  protected ClientState createClientState( FlowProcess flowProcess )
    {
    CascadingServices services = flowProcess.getCurrentSession().getCascadingServices();

    if( services == null )
      return ClientState.NULL;

    return services.createClientState( getID() );
    }

  public FlowStepJob<Config> getFlowStepJob()
    {
    return flowStepJob;
    }

  public FlowStepJob<Config> getCreateFlowStepJob( FlowProcess<Config> flowProcess, Config parentConfig )
    {
    if( flowStepJob != null )
      return flowStepJob;

    if( flowProcess == null )
      return null;

    Config initializedConfig = createInitializedConfig( flowProcess, parentConfig );

    setConfig( initializedConfig );

    ClientState clientState = createClientState( flowProcess );

    flowStepJob = createFlowStepJob( clientState, flowProcess, initializedConfig );

    return flowStepJob;
    }

  protected abstract FlowStepJob createFlowStepJob( ClientState clientState, FlowProcess<Config> flowProcess, Config initializedStepConfig );

  protected void initConfFromNodeConfigDef( ElementGraph nodeElementGraph, ConfigDef.Setter setter )
    {
    nodeElementGraph = ElementGraphs.asExtentMaskedSubGraph( nodeElementGraph );

    ElementGraph stepElementGraph = ElementGraphs.asExtentMaskedSubGraph( getElementGraph() );

    // applies each mode in order, topologically
    for( ConfigDef.Mode mode : ConfigDef.Mode.values() )
      {
      Iterator<FlowElement> iterator = ElementGraphs.getTopologicalIterator( nodeElementGraph );

      while( iterator.hasNext() )
        {
        FlowElement element = iterator.next();

        while( element != null )
          {
          // intentionally skip any element that spans downstream nodes, like a GroupBy
          // this way GroupBy is applied on the inbound side (where partitioning happens)
          // not the outbound side.
          // parent sub-assemblies (like Unique) will be applied if they have leading Pipes to the current spanning Pipe
          if( elementSpansDownStream( stepElementGraph, nodeElementGraph, element ) )
            {
            element = null;
            continue;
            }

          if( element instanceof ScopedElement && ( (ScopedElement) element ).hasNodeConfigDef() )
            ( (ScopedElement) element ).getNodeConfigDef().apply( mode, setter );

          // walk up the sub-assembly parent hierarchy
          if( element instanceof Pipe )
            element = ( (Pipe) element ).getParent();
          else
            element = null;
          }
        }
      }
    }

  private boolean elementSpansDownStream( ElementGraph stepElementGraph, ElementGraph nodeElementGraph, FlowElement element )
    {
    boolean spansNodes = !( element instanceof SubAssembly );

    if( spansNodes )
      spansNodes = nodeElementGraph.outDegreeOf( element ) == 0 && stepElementGraph.outDegreeOf( element ) > 0;

    return spansNodes;
    }

  protected void initConfFromStepConfigDef( ConfigDef.Setter setter )
    {
    ElementGraph stepElementGraph = ElementGraphs.asExtentMaskedSubGraph( getElementGraph() );

    // applies each mode in order, topologically
    for( ConfigDef.Mode mode : ConfigDef.Mode.values() )
      {
      Iterator<FlowElement> iterator = ElementGraphs.getTopologicalIterator( stepElementGraph );

      while( iterator.hasNext() )
        {
        FlowElement element = iterator.next();

        while( element != null )
          {
          if( element instanceof ScopedElement && ( (ScopedElement) element ).hasStepConfigDef() )
            ( (ScopedElement) element ).getStepConfigDef().apply( mode, setter );

          // walk up the sub-assembly parent hierarchy
          if( element instanceof Pipe )
            element = ( (Pipe) element ).getParent();
          else
            element = null;
          }
        }
      }
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( object == null || getClass() != object.getClass() )
      return false;

    BaseFlowStep flowStep = (BaseFlowStep) object;

    if( id != null ? !id.equals( flowStep.id ) : flowStep.id != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    return id != null ? id.hashCode() : 0;
    }

  @Override
  public String toString()
    {
    StringBuffer buffer = new StringBuffer();

    buffer.append( getClass().getSimpleName() );
    buffer.append( "[name: " ).append( getName() ).append( "]" );

    return buffer.toString();
    }

  @Override
  public final boolean isInfoEnabled()
    {
    return getLogger().isInfoEnabled();
    }

  private ProcessLogger getLogger()
    {
    if( flow != null && flow instanceof ProcessLogger )
      return (ProcessLogger) flow;

    return ProcessLogger.NULL;
    }

  @Override
  public final boolean isDebugEnabled()
    {
    return ( getLogger() ).isDebugEnabled();
    }

  @Override
  public void logDebug( String message, Object... arguments )
    {
    getLogger().logDebug( message, arguments );
    }

  @Override
  public void logInfo( String message, Object... arguments )
    {
    getLogger().logInfo( message, arguments );
    }

  @Override
  public void logWarn( String message )
    {
    getLogger().logWarn( message );
    }

  @Override
  public void logWarn( String message, Throwable throwable )
    {
    getLogger().logWarn( message, throwable );
    }

  @Override
  public void logWarn( String message, Object... arguments )
    {
    getLogger().logWarn( message, arguments );
    }

  @Override
  public void logError( String message, Object... arguments )
    {
    getLogger().logError( message, arguments );
    }

  @Override
  public void logError( String message, Throwable throwable )
    {
    getLogger().logError( message, throwable );
    }

  /**
   * Class SafeFlowStepListener safely calls a wrapped FlowStepListener.
   * <p/>
   * This is done for a few reasons, the primary reason is so exceptions thrown by the Listener
   * can be caught by the calling Thread. Since Flow is asynchronous, much of the work is done in the run() method
   * which in turn is run in a new Thread.
   */
  private class SafeFlowStepListener implements FlowStepListener
    {
    /** Field flowListener */
    final FlowStepListener flowStepListener;
    /** Field throwable */
    Throwable throwable;

    private SafeFlowStepListener( FlowStepListener flowStepListener )
      {
      this.flowStepListener = flowStepListener;
      }

    public void onStepStarting( FlowStep flowStep )
      {
      try
        {
        flowStepListener.onStepStarting( flowStep );
        }
      catch( Throwable throwable )
        {
        handleThrowable( throwable );
        }
      }

    public void onStepStopping( FlowStep flowStep )
      {
      try
        {
        flowStepListener.onStepStopping( flowStep );
        }
      catch( Throwable throwable )
        {
        handleThrowable( throwable );
        }
      }

    public void onStepCompleted( FlowStep flowStep )
      {
      try
        {
        flowStepListener.onStepCompleted( flowStep );
        }
      catch( Throwable throwable )
        {
        handleThrowable( throwable );
        }
      }

    public void onStepRunning( FlowStep flowStep )
      {
      try
        {
        flowStepListener.onStepRunning( flowStep );
        }
      catch( Throwable throwable )
        {
        handleThrowable( throwable );
        }
      }

    public boolean onStepThrowable( FlowStep flowStep, Throwable flowStepThrowable )
      {
      try
        {
        return flowStepListener.onStepThrowable( flowStep, flowStepThrowable );
        }
      catch( Throwable throwable )
        {
        handleThrowable( throwable );
        }

      return false;
      }

    private void handleThrowable( Throwable throwable )
      {
      this.throwable = throwable;

      logWarn( String.format( "flow step listener %s threw throwable", flowStepListener ), throwable );
      }

    public boolean equals( Object object )
      {
      if( object instanceof BaseFlowStep.SafeFlowStepListener )
        return flowStepListener.equals( ( (BaseFlowStep.SafeFlowStepListener) object ).flowStepListener );

      return flowStepListener.equals( object );
      }

    public int hashCode()
      {
      return flowStepListener.hashCode();
      }
    }
  }