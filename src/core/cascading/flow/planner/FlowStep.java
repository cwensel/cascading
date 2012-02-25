/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import cascading.flow.Flow;
import cascading.flow.FlowElement;
import cascading.flow.FlowProcess;
import cascading.flow.Scope;
import cascading.management.CascadingServices;
import cascading.management.ClientState;
import cascading.operation.Operation;
import cascading.pipe.ConfigDef;
import cascading.pipe.Group;
import cascading.pipe.Join;
import cascading.pipe.Operator;
import cascading.pipe.Pipe;
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
public abstract class FlowStep<Config> implements Serializable
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( FlowStep.class );

  /** Field properties */
  private Map<Object, Object> properties = null;

  /** Field flow */
  private transient Flow flow;
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
  protected Tap tempSink; // used if we need to bypass

  /** Field groups */
  private final List<Group> groups = new ArrayList<Group>();

  // sources streamed into join - not necessarily all sources
  protected final Map<Join, Tap> streamedSourceByJoin = new LinkedHashMap<Join, Tap>();
  // sources accumulated by join
  protected final Map<Join, Set<Tap>> accumulatedSourcesByJoin = new LinkedHashMap<Join, Set<Tap>>();

  private transient FlowStepJob flowStepJob;

  protected FlowStep( String name, int stepNum )
    {
    this.name = name;
    this.stepNum = stepNum;
    }

  /**
   * Method getId returns the id of this FlowStep object.
   *
   * @return the id (type int) of this FlowStep object.
   */
  public String getID()
    {
    if( id == null )
      id = Util.createUniqueID( getName() );

    return id;
    }

  public int getStepNum()
    {
    return stepNum;
    }

  /**
   * Method getName returns the name of this FlowStep object.
   *
   * @return the name (type String) of this FlowStep object.
   */
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

  public void setFlow( Flow flow )
    {
    this.flow = flow;
    this.flowID = flow.getID();
    this.flowName = flow.getName();
    }

  protected Flow getFlow()
    {
    return flow;
    }

  public String getFlowID()
    {
    return flowID;
    }

  /**
   * Method getParentFlowName returns the parentFlowName of this FlowStep object.
   *
   * @return the parentFlowName (type Flow) of this FlowStep object.
   */
  public String getFlowName()
    {
    return flowName;
    }

  /**
   * Method setParentFlowName sets the parentFlowName of this FlowStep object.
   *
   * @param flowName the parentFlowName of this FlowStep object.
   */
  public void setFlowName( String flowName )
    {
    this.flowName = flowName;
    }

  public Config getConfig()
    {
    return conf;
    }

  public void setConf( Config conf )
    {
    this.conf = conf;
    }

  /**
   * Method getStepDisplayName returns the stepDisplayName of this FlowStep object.
   *
   * @return the stepName (type String) of this FlowStep object.
   */
  public String getStepDisplayName()
    {
    return String.format( "%s[%s]", getFlowName(), getName() );
    }

  /**
   * Method getSubmitPriority returns the submitPriority of this FlowStep object.
   * <p/>
   * 10 is lowest, 1 is the highest, 5 is the default.
   *
   * @return the submitPriority (type int) of this FlowStep object.
   */
  public int getSubmitPriority()
    {
    return submitPriority;
    }

  /**
   * Method setSubmitPriority sets the submitPriority of this FlowStep object.
   * <p/>
   * 10 is lowest, 1 is the highest, 5 is the default.
   *
   * @param submitPriority the submitPriority of this FlowStep object.
   */
  public void setSubmitPriority( int submitPriority )
    {
    if( submitPriority < 1 || submitPriority > 10 )
      throw new IllegalArgumentException( "submitPriority must be between 1 and 10 inclusive, was: " + submitPriority );

    this.submitPriority = submitPriority;
    }

  public SimpleDirectedGraph<FlowElement, Scope> getGraph()
    {
    return graph;
    }

  public Group getGroup()
    {
    if( groups.isEmpty() )
      return null;

    if( groups.size() > 1 )
      throw new IllegalStateException( "more than one group" );

    return groups.get( 0 );
    }

  public List<Group> getGroups()
    {
    return groups;
    }

  public void addGroup( Group group )
    {
    if( !groups.contains( group ) )
      groups.add( group );
    }

  public Map<Join, Tap> getStreamedSourceByJoin()
    {
    return streamedSourceByJoin;
    }

  public void addStreamedSourceFor( Join join, Tap streamedSource )
    {
    streamedSourceByJoin.put( join, streamedSource );
    }

  public Set<Tap> getAllAccumulatedSources()
    {
    HashSet<Tap> set = new HashSet<Tap>();

    for( Set<Tap> taps : accumulatedSourcesByJoin.values() )
      set.addAll( taps );

    return set;
    }

  public void addAccumulatedSourceFor( Join join, Tap accumulatedSource )
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

  public Set<Tap> getSources()
    {
    return Collections.unmodifiableSet( new HashSet<Tap>( sources.keySet() ) );
    }

  public Set<Tap> getSinks()
    {
    return Collections.unmodifiableSet( new HashSet<Tap>( sinks.keySet() ) );
    }

  public Tap getSink()
    {
    if( sinks.size() != 1 )
      throw new IllegalStateException( "more than one sink" );

    return sinks.keySet().iterator().next();
    }

  public Set<String> getSourceName( Tap source )
    {
    return Collections.unmodifiableSet( sources.get( source ) );
    }

  public Set<String> getSinkName( Tap sink )
    {
    return Collections.unmodifiableSet( sinks.get( sink ) );
    }

  public abstract Set<Tap> getTraps();

  public abstract Tap getTrap( String name );

  void commitSinks()
    {
    flow.commitTaps( sinks.keySet() );
    }

  void rollbackSinks()
    {
    flow.rollbackTaps( sinks.keySet() );
    }

  /**
   * Method getProperties returns the properties of this FlowStep object.
   * </br>
   * These properties are local only to this step, use this method to set additional properties and configuration
   * for the underlying platform.
   *
   * @return the properties (type Map<Object, Object>) of this FlowStep object.
   */
  public Map<Object, Object> getProperties()
    {
    if( properties == null )
      properties = new Properties();

    return properties;
    }

  /**
   * Method setProperties sets the properties of this FlowStep object.
   *
   * @param properties the properties of this FlowStep object.
   */
  public void setProperties( Map<Object, Object> properties )
    {
    this.properties = properties;
    }

  /**
   * Method hasProperties returns {@code true} if there are properties associated with this FlowStep.
   *
   * @return boolean
   */
  public boolean hasProperties()
    {
    return properties != null && !properties.isEmpty();
    }

  public abstract Config getInitializedConfig( FlowProcess<Config> flowProcess, Config parentConfig );

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
    Set<Join> joins = new HashSet<Join>();
    List<GraphPath<FlowElement, Scope>> paths = getPathsBetween( from, to );

    for( GraphPath<FlowElement, Scope> path : paths )
      {
      for( FlowElement flowElement : Graphs.getPathVertexList( path ) )
        {
        if( flowElement instanceof Join )
          joins.add( (Join) flowElement );
        }
      }

    Set<Tap> tributaries = new HashSet<Tap>();

    for( Join join : joins )
      {
      for( Tap source : sources.keySet() )
        {
        if( !getPathsBetween( source, join ).isEmpty() )
          tributaries.add( source );
        }
      }

    return tributaries;
    }

  private List<GraphPath<FlowElement, Scope>> getPathsBetween( FlowElement from, FlowElement to )
    {
    KShortestPaths<FlowElement, Scope> paths = new KShortestPaths<FlowElement, Scope>( graph, from, Integer.MAX_VALUE );

    if( paths == null )
      return Collections.EMPTY_LIST;

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

  /**
   * Returns true if this FlowStep contains a pipe/branch with the given name.
   *
   * @param pipeName
   * @return
   */
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

  public abstract void clean( Config config );

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( object == null || getClass() != object.getClass() )
      return false;

    FlowStep flowStep = (FlowStep) object;

    if( name != null ? !name.equals( flowStep.name ) : flowStep.name != null )
      return false;

    return true;
    }

  protected ClientState createClientState( FlowProcess flowProcess )
    {
    CascadingServices services = flowProcess.getCurrentSession().getCascadingServices();
    return services.createClientState( getID() );
    }

  public FlowStepJob getFlowStepJob( FlowProcess<Config> flowProcess, Config parentConfig )
    {
    if( flowStepJob != null )
      return flowStepJob;

    if( flowProcess == null )
      return null;

    flowStepJob = createFlowStepJob( flowProcess, parentConfig );

    return flowStepJob;
    }

  protected abstract FlowStepJob createFlowStepJob( FlowProcess<Config> flowProcess, Config parentConfig );

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

  protected void initConfFromPipes( ConfigDef.Setter setter )
    {
    // applies each mode in order, topologically
    for( ConfigDef.Mode mode : ConfigDef.Mode.values() )
      {
      TopologicalOrderIterator<FlowElement, Scope> iterator = getTopologicalOrderIterator();

      while( iterator.hasNext() )
        {
        FlowElement element = iterator.next();

        if( !( element instanceof Pipe ) )
          continue;

        Pipe pipe = (Pipe) element;

        if( !pipe.hasProcessConfigDef() )
          continue;

        pipe.getProcessConfigDef().apply( mode, setter );
        }
      }
    }
  }
