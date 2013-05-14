/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import cascading.flow.Flow;
import cascading.flow.FlowElement;
import cascading.flow.FlowException;
import cascading.flow.FlowProcess;
import cascading.flow.FlowStep;
import cascading.management.CascadingServices;
import cascading.management.state.ClientState;
import cascading.operation.Operation;
import cascading.pipe.Group;
import cascading.pipe.HashJoin;
import cascading.pipe.Merge;
import cascading.pipe.Operator;
import cascading.pipe.Pipe;
import cascading.property.ConfigDef;
import cascading.stats.FlowStepStats;
import cascading.tap.Tap;
import cascading.util.Util;
import org.jgrapht.GraphPath;
import org.jgrapht.Graphs;
import org.jgrapht.alg.KShortestPaths;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public abstract class BaseFlowStep<Config> implements Serializable, FlowStep<Config>
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( FlowStep.class );

  /** Field flow */
  private transient Flow<Config> flow;
  /** Field flowName */
  private String flowName;
  /** Field flowID */
  private String flowID;

  private transient Config conf;

  /** Field submitPriority */
  private int submitPriority = 5;

  /** Field name */
  String name;
  /** Field id */
  private String id;
  private final int stepNum;

  /** Field graph */
  private final SimpleDirectedGraph<FlowElement, Scope> graph = new SimpleDirectedGraph<FlowElement, Scope>( Scope.class );

  /** Field sources */
  protected final Map<Tap, Set<String>> sources = new HashMap<Tap, Set<String>>(); // all sources
  /** Field sink */
  protected final Map<Tap, Set<String>> sinks = new HashMap<Tap, Set<String>>(); // all sinks

  /** Field tempSink */
  protected Tap tempSink; // used if we need to bypass the filesystem

  /** Field groups */
  private final List<Group> groups = new ArrayList<Group>();

  // sources streamed into join - not necessarily all sources
  protected final Map<HashJoin, Tap> streamedSourceByJoin = new LinkedHashMap<HashJoin, Tap>();
  // sources accumulated by join
  protected final Map<HashJoin, Set<Tap>> accumulatedSourcesByJoin = new LinkedHashMap<HashJoin, Set<Tap>>();

  private transient FlowStepJob<Config> flowStepJob;

  protected BaseFlowStep( String name, int stepNum )
    {
    setName( name );
    this.stepNum = stepNum;
    }

  @Override
  public String getID()
    {
    if( id == null )
      id = Util.createUniqueID();

    return id;
    }

  @Override
  public int getStepNum()
    {
    return stepNum;
    }

  @Override
  public String getName()
    {
    return name;
    }

  void setName( String name )
    {
    if( name == null || name.isEmpty() )
      throw new IllegalArgumentException( "step name may not be null or empty" );

    this.name = name;
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
    return conf;
    }

  protected void setConf( Config conf )
    {
    this.conf = conf;
    }

  @Override
  public String getStepDisplayName()
    {
    return getStepDisplayName( Util.ID_LENGTH );
    }

  protected String getStepDisplayName( int idLength )
    {
    if( idLength > Util.ID_LENGTH )
      idLength = Util.ID_LENGTH;

    String flowID = getFlowID().substring( 0, idLength );
    String stepID = getID().substring( 0, idLength );

    return String.format( "[%s/%s] %s/%s", flowID, stepID, getFlowName(), getName() );
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
  public FlowStepStats getFlowStepStats()
    {
    return flowStepJob.getStepStats();
    }

  public SimpleDirectedGraph<FlowElement, Scope> getGraph()
    {
    return graph;
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
  public List<Group> getGroups()
    {
    return groups;
    }

  public void addGroup( Group group )
    {
    if( !groups.contains( group ) )
      groups.add( group );
    }

  @Override
  public Map<HashJoin, Tap> getStreamedSourceByJoin()
    {
    return streamedSourceByJoin;
    }

  public void addStreamedSourceFor( HashJoin join, Tap streamedSource )
    {
    streamedSourceByJoin.put( join, streamedSource );
    }

  @Override
  public Set<Tap> getAllAccumulatedSources()
    {
    HashSet<Tap> set = new HashSet<Tap>();

    for( Set<Tap> taps : accumulatedSourcesByJoin.values() )
      set.addAll( taps );

    return set;
    }

  public void addAccumulatedSourceFor( HashJoin join, Tap accumulatedSource )
    {
    if( !accumulatedSourcesByJoin.containsKey( join ) )
      accumulatedSourcesByJoin.put( join, new HashSet<Tap>() );

    accumulatedSourcesByJoin.get( join ).add( accumulatedSource );
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
  public Set<Tap> getSources()
    {
    return Collections.unmodifiableSet( new HashSet<Tap>( sources.keySet() ) );
    }

  @Override
  public Set<Tap> getSinks()
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
      if( LOG.isInfoEnabled() )
        logInfo( "source modification date at: " + new Date( sourceModified ) ); // not oldest, we didnt check them all
      }
    }

  long getSinkModified() throws IOException
    {
    long sinkModified = Util.getSinkModified( getConfig(), sinks.keySet() );

    if( LOG.isInfoEnabled() )
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

        logError( message, null );

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

        logError( message, null );

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

  protected abstract Config getInitializedConfig( FlowProcess<Config> flowProcess, Config parentConfig );

  /**
   * Method getPreviousScopes returns the previous Scope instances. If the flowElement is a Group (specifically a CoGroup),
   * there will be more than one instance.
   *
   * @param flowElement of type FlowElement
   * @return Set<Scope>
   */
  public Set<Scope> getPreviousScopes( FlowElement flowElement )
    {
    return getGraph().incomingEdgesOf( flowElement );
    }

  /**
   * Method getNextScope returns the next Scope instance in the graph. There will always only be one next.
   *
   * @param flowElement of type FlowElement
   * @return Scope
   */
  public Scope getNextScope( FlowElement flowElement )
    {
    Set<Scope> set = getGraph().outgoingEdgesOf( flowElement );

    if( set.size() != 1 )
      throw new IllegalStateException( "should only be one scope after current flow element: " + flowElement + " found: " + set.size() );

    return set.iterator().next();
    }

  public Scope getScopeFor( FlowElement sourceElement, FlowElement targetElement )
    {
    return getGraph().getEdge( sourceElement, targetElement );
    }

  public Set<Scope> getNextScopes( FlowElement flowElement )
    {
    return getGraph().outgoingEdgesOf( flowElement );
    }

  public FlowElement getNextFlowElement( Scope scope )
    {
    return getGraph().getEdgeTarget( scope );
    }

  public TopologicalOrderIterator<FlowElement, Scope> getTopologicalOrderIterator()
    {
    return new TopologicalOrderIterator<FlowElement, Scope>( graph );
    }

  public List<FlowElement> getSuccessors( FlowElement element )
    {
    return Graphs.successorListOf( graph, element );
    }

  public Set<Tap> getJoinTributariesBetween( FlowElement from, FlowElement to )
    {
    Set<HashJoin> joins = new HashSet<HashJoin>();
    Set<Merge> merges = new HashSet<Merge>();

    List<GraphPath<FlowElement, Scope>> paths = getPathsBetween( from, to );

    for( GraphPath<FlowElement, Scope> path : paths )
      {
      for( FlowElement flowElement : Graphs.getPathVertexList( path ) )
        {
        if( flowElement instanceof HashJoin )
          joins.add( (HashJoin) flowElement );

        if( flowElement instanceof Merge )
          merges.add( (Merge) flowElement );
        }
      }

    Set<Tap> tributaries = new HashSet<Tap>();

    for( HashJoin join : joins )
      {
      for( Tap source : sources.keySet() )
        {
        List<GraphPath<FlowElement, Scope>> joinPaths = new LinkedList( getPathsBetween( source, join ) );

        ListIterator<GraphPath<FlowElement, Scope>> iterator = joinPaths.listIterator();

        while( iterator.hasNext() )
          {
          GraphPath<FlowElement, Scope> joinPath = iterator.next();

          if( !Collections.disjoint( Graphs.getPathVertexList( joinPath ), merges ) )
            iterator.remove();
          }

        if( !joinPaths.isEmpty() )
          tributaries.add( source );
        }
      }

    return tributaries;
    }

  private List<GraphPath<FlowElement, Scope>> getPathsBetween( FlowElement from, FlowElement to )
    {
    KShortestPaths<FlowElement, Scope> paths = new KShortestPaths<FlowElement, Scope>( graph, from, Integer.MAX_VALUE );
    List<GraphPath<FlowElement, Scope>> results = paths.getPaths( to );

    if( results == null )
      return Collections.EMPTY_LIST;

    return results;
    }

  public Collection<Operation> getAllOperations()
    {
    Set<FlowElement> vertices = getGraph().vertexSet();
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
    Set<FlowElement> vertices = getGraph().vertexSet();

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

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( object == null || getClass() != object.getClass() )
      return false;

    BaseFlowStep flowStep = (BaseFlowStep) object;

    if( name != null ? !name.equals( flowStep.name ) : flowStep.name != null )
      return false;

    return true;
    }

  protected ClientState createClientState( FlowProcess flowProcess )
    {
    CascadingServices services = flowProcess.getCurrentSession().getCascadingServices();
    return services.createClientState( getID() );
    }

  public FlowStepJob<Config> getFlowStepJob( FlowProcess<Config> flowProcess, Config parentConfig )
    {
    if( flowStepJob != null )
      return flowStepJob;

    if( flowProcess == null )
      return null;

    flowStepJob = createFlowStepJob( flowProcess, parentConfig );

    return flowStepJob;
    }

  protected abstract FlowStepJob createFlowStepJob( FlowProcess<Config> flowProcess, Config parentConfig );

  protected void initConfFromProcessConfigDef( ConfigDef.Setter setter )
    {
    // applies each mode in order, topologically
    for( ConfigDef.Mode mode : ConfigDef.Mode.values() )
      {
      TopologicalOrderIterator<FlowElement, Scope> iterator = getTopologicalOrderIterator();

      while( iterator.hasNext() )
        {
        FlowElement element = iterator.next();

        while( element != null )
          {
          if( element.hasStepConfigDef() )
            element.getStepConfigDef().apply( mode, setter );

          if( element instanceof Pipe )
            element = ( (Pipe) element ).getParent();
          else
            element = null;
          }
        }
      }
    }

  @Override
  public int hashCode()
    {
    return name != null ? name.hashCode() : 0;
    }

  @Override
  public String toString()
    {
    StringBuffer buffer = new StringBuffer();

    buffer.append( getClass().getSimpleName() );
    buffer.append( "[name: " ).append( getName() ).append( "]" );

    return buffer.toString();
    }

  public final boolean isInfoEnabled()
    {
    return LOG.isInfoEnabled();
    }

  public final boolean isDebugEnabled()
    {
    return LOG.isDebugEnabled();
    }

  public void logDebug( String message )
    {
    LOG.debug( "[" + Util.truncate( getFlowName(), 25 ) + "] " + message );
    }

  public void logInfo( String message )
    {
    LOG.info( "[" + Util.truncate( getFlowName(), 25 ) + "] " + message );
    }

  public void logWarn( String message )
    {
    LOG.warn( "[" + Util.truncate( getFlowName(), 25 ) + "] " + message );
    }

  public void logWarn( String message, Throwable throwable )
    {
    LOG.warn( "[" + Util.truncate( getFlowName(), 25 ) + "] " + message, throwable );
    }

  public void logError( String message, Throwable throwable )
    {
    LOG.error( "[" + Util.truncate( getFlowName(), 25 ) + "] " + message, throwable );
    }
  }
