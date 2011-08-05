/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
 */

package cascading.flow.planner;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import cascading.flow.FlowElement;
import cascading.flow.FlowProcess;
import cascading.flow.Scope;
import cascading.operation.Operation;
import cascading.pipe.Every;
import cascading.pipe.Group;
import cascading.pipe.Operator;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.util.Util;
import org.jgrapht.Graphs;
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
  /** Field parentFlowName */
  private String parentFlowName;
  private String parentFlowID;

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
  protected final Map<String, Tap> sources = new TreeMap<String, Tap>();
  /** Field sink */
  protected final Map<String, Tap> sinks = new TreeMap<String, Tap>();
  /** Field tempSink */
  protected Tap tempSink; // used if we need to bypass
  /** Field groups */
  private final List<Group> groups = new ArrayList<Group>();

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

  public String getParentFlowID()
    {
    return parentFlowID;
    }

  public void setParentFlowID( String parentFlowID )
    {
    this.parentFlowID = parentFlowID;
    }

  /**
   * Method getParentFlowName returns the parentFlowName of this FlowStep object.
   *
   * @return the parentFlowName (type Flow) of this FlowStep object.
   */
  public String getParentFlowName()
    {
    return parentFlowName;
    }

  /**
   * Method setParentFlowName sets the parentFlowName of this FlowStep object.
   *
   * @param parentFlowName the parentFlowName of this FlowStep object.
   */
  public void setParentFlowName( String parentFlowName )
    {
    this.parentFlowName = parentFlowName;
    }

  /**
   * Method getStepName returns the stepName of this FlowStep object.
   *
   * @return the stepName (type String) of this FlowStep object.
   */
  public String getStepName()
    {
    return String.format( "%s[%s]", getParentFlowName(), getName() );
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

  public Map<String, Tap> getSourceMap()
    {
    return sources;
    }

  public Map<String, Tap> getSinkMap()
    {
    return sinks;
    }

  public void addSource( String name, Tap source )
    {
    sources.put( name, source );
    }

  public void addSink( String name, Tap sink )
    {
    sinks.put( name, sink );
    }

  public Set<Tap> getSources()
    {
    return Collections.unmodifiableSet( new HashSet<Tap>( sources.values() ) );
    }

  public Set<Tap> getSinks()
    {
    return Collections.unmodifiableSet( new HashSet<Tap>( sinks.values() ) );
    }

  public Tap getSink()
    {
    if( sinks.size() != 1 )
      throw new IllegalStateException( "more than one sink" );

    return sinks.values().iterator().next();
    }

  public Set<String> getSourceName( Tap source )
    {
    Set<String> names = new HashSet<String>();

    for( Map.Entry<String, Tap> entry : sources.entrySet() )
      {
      if( entry.getValue().equals( source ) )
        names.add( entry.getKey() );
      }

    return names;
    }

  public Set<String> getSinkName( Tap source )
    {
    Set<String> names = new HashSet<String>();

    for( Map.Entry<String, Tap> entry : sinks.entrySet() )
      {
      if( entry.getValue().equals( source ) )
        names.add( entry.getKey() );
      }

    return names;
    }

  public abstract Tap getTrap( String name );

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

  public abstract Config getInitializedConfig( FlowProcess<Config> flowProcess, Config parentConfig ) throws IOException;

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

  public Every[] getAllNextEveryOf( Every every )
    {
    List<Every> elements = new ArrayList<Every>();

    elements.add( every );

    // should be no branching
    handleNext( elements, every );

    return elements.toArray( new Every[ elements.size() ] );
    }

  private void handleNext( List<Every> elements, Every element )
    {
    List<FlowElement> successors = getSuccessors( element );
    FlowElement successor = successors.get( 0 );

    if( !( successor instanceof Every ) )
      return;

    if( successors.size() != 1 )
      throw new IllegalStateException( "no branching allowed after an Every" );

    elements.add( (Every) successor );

    handleNext( elements, (Every) successor );
    }

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

  public abstract FlowStepJob createFlowStepJob( FlowProcess<Config> flowProcess, Config parentConfig ) throws IOException;

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
    LOG.debug( "[" + Util.truncate( getParentFlowName(), 25 ) + "] " + message );
    }

  public void logInfo( String message )
    {
    LOG.info( "[" + Util.truncate( getParentFlowName(), 25 ) + "] " + message );
    }

  public void logWarn( String message )
    {
    LOG.warn( "[" + Util.truncate( getParentFlowName(), 25 ) + "] " + message );
    }

  public void logWarn( String message, Throwable throwable )
    {
    LOG.warn( "[" + Util.truncate( getParentFlowName(), 25 ) + "] " + message, throwable );
    }

  public void logError( String message, Throwable throwable )
    {
    LOG.error( "[" + Util.truncate( getParentFlowName(), 25 ) + "] " + message, throwable );
    }
  }
