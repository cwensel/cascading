/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import cascading.operation.Operation;
import cascading.pipe.Group;
import cascading.pipe.Operator;
import cascading.tap.Tap;
import cascading.tap.TempHfs;
import cascading.tap.hadoop.Hadoop18TapUtil;
import cascading.tap.hadoop.MultiInputFormat;
import cascading.tap.hadoop.TapIterator;
import cascading.tuple.IndexTuple;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TuplePair;
import cascading.tuple.hadoop.GroupingComparator;
import cascading.tuple.hadoop.GroupingPartitioner;
import cascading.tuple.hadoop.ReverseTupleComparator;
import cascading.tuple.hadoop.ReverseTuplePairComparator;
import cascading.tuple.hadoop.TupleComparator;
import cascading.tuple.hadoop.TuplePairComparator;
import cascading.tuple.hadoop.TupleSerialization;
import cascading.util.Util;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.log4j.Logger;
import org.jgrapht.graph.SimpleDirectedGraph;

/**
 * Class FlowStep is an internal representation of a given Job to be executed on a remote cluster. During
 * planning, pipe assemblies are broken down into "steps" and encapsulated in this class.
 */
public class FlowStep implements Serializable
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( FlowStep.class );

  /** Field parentFlowName */
  private String parentFlowName;

  /** Field name */
  final String name;
  /** Field graph */
  final SimpleDirectedGraph<FlowElement, Scope> graph = new SimpleDirectedGraph<FlowElement, Scope>( Scope.class );

  /** Field sources */
  final Map<Tap, String> sources = new HashMap<Tap, String>();   // all sources and all sinks must have same scheme
  /** Field sink */
  Tap sink;
  /** Field mapperTraps */
  public final Map<String, Tap> mapperTraps = new HashMap<String, Tap>();
  /** Field reducerTraps */
  public final Map<String, Tap> reducerTraps = new HashMap<String, Tap>();
  /** Field tempSink */
  TempHfs tempSink; // used if we need to bypass
  /** Field group */
  public Group group;

  FlowStep( String name )
    {
    this.name = name;
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

  /**
   * Method getParentFlowName returns the parentFlowName of this FlowStep object.
   *
   * @return the parentFlowName (type Flow) of this FlowStep object.
   */
  public String getParentFlowName()
    {
    return parentFlowName;
    }

  void setParentFlowName( String parentFlowName )
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

  JobConf getJobConf() throws IOException
    {
    return getJobConf( null );
    }

  JobConf getJobConf( JobConf parentConf ) throws IOException
    {
    JobConf conf = parentConf == null ? new JobConf() : new JobConf( parentConf );

    conf.setJobName( getStepName() );

    conf.setOutputKeyClass( Tuple.class );
    conf.setOutputValueClass( Tuple.class );

    conf.setMapperClass( FlowMapper.class );
    conf.setReducerClass( FlowReducer.class );

    // set for use by the shuffling phase
    TupleSerialization.setSerializations( conf );

    initFromSources( conf );

    initFromSink( conf );

    initFromTraps( conf );

    if( sink.getScheme().getNumSinkParts() != 0 )
      {
      // if no reducer, set num map tasks to control parts
      if( group != null )
        conf.setNumReduceTasks( sink.getScheme().getNumSinkParts() );
      else
        conf.setNumMapTasks( sink.getScheme().getNumSinkParts() );
      }

    conf.setOutputKeyComparatorClass( TupleComparator.class );

    if( group == null )
      {
      conf.setNumReduceTasks( 0 ); // disable reducers
      }
    else
      {
      // must set map output defaults when performing a reduce
      conf.setMapOutputKeyClass( Tuple.class );
      conf.setMapOutputValueClass( Tuple.class );

      // handles the case the groupby sort should be reversed
      if( group.isSortReversed() )
        conf.setOutputKeyComparatorClass( ReverseTupleComparator.class );

      if( !group.isGroupBy() )
        conf.setMapOutputValueClass( IndexTuple.class );

      if( group.isSorted() )
        {
        conf.setPartitionerClass( GroupingPartitioner.class );
        conf.setMapOutputKeyClass( TuplePair.class );

        if( group.isSortReversed() )
          conf.setOutputKeyComparatorClass( ReverseTuplePairComparator.class );
        else
          conf.setOutputKeyComparatorClass( TuplePairComparator.class );

        // no need to supply a reverse comparator, only equality is checked
        conf.setOutputValueGroupingComparator( GroupingComparator.class );
        }
      }

    // perform last so init above will pass to tasks
    conf.set( "cascading.flow.step", Util.serializeBase64( this ) );

    return conf;
    }

  private void initFromTraps( JobConf conf ) throws IOException
    {
    initFromTraps( conf, mapperTraps );
    initFromTraps( conf, reducerTraps );
    }

  private void initFromTraps( JobConf conf, Map<String, Tap> traps ) throws IOException
    {
    if( !traps.isEmpty() )
      {
      JobConf trapConf = new JobConf( conf );

      for( Tap tap : traps.values() )
        {
        tap.sinkInit( trapConf );
        }
      }
    }

  private void initFromSources( JobConf conf ) throws IOException
    {
    JobConf[] fromJobs = new JobConf[sources.size()];
    int i = 0;

    for( Tap tap : sources.keySet() )
      {
      fromJobs[ i ] = new JobConf( conf );
      tap.sourceInit( fromJobs[ i ] );
      fromJobs[ i ].set( "cascading.step.source", Util.serializeBase64( tap ) );
      i++;
      }

    MultiInputFormat.addInputFormat( conf, fromJobs );
    }

  private void initFromSink( JobConf conf ) throws IOException
    {
    if( tempSink != null )
      tempSink.sinkInit( conf );
    else
      sink.sinkInit( conf );
    }

  public TapIterator openSourceForRead( JobConf conf ) throws IOException
    {
    return new TapIterator( sources.keySet().iterator().next(), conf );
    }

  public TupleEntryIterator openSinkForRead( JobConf conf ) throws IOException
    {
    return sink.openForRead( conf );
    }

  public Tap getMapperTrap( String name )
    {
    return mapperTraps.get( name );
    }

  public Tap getReducerTrap( String name )
    {
    return reducerTraps.get( name );
    }

  /**
   * Method getPreviousScopes returns the previous Scope instances. If the flowElement is a Group (specifically a CoGroup),
   * there will be more than one instance.
   *
   * @param flowElement of type FlowElement
   * @return Set<Scope>
   */
  public Set<Scope> getPreviousScopes( FlowElement flowElement )
    {
    return graph.incomingEdgesOf( flowElement );
    }

  /**
   * Method getNextScope returns the next Scope instance in the graph. There will always only be one next.
   *
   * @param flowElement of type FlowElement
   * @return Scope
   */
  public Scope getNextScope( FlowElement flowElement )
    {
    Set<Scope> set = graph.outgoingEdgesOf( flowElement );

    if( set.size() != 1 )
      throw new IllegalStateException( "should only be one scope after current flow element: " + flowElement + " found: " + set.size() );

    return set.iterator().next();
    }

  public Set<Scope> getNextScopes( FlowElement flowElement )
    {
    return graph.outgoingEdgesOf( flowElement );
    }

  public FlowElement getNextFlowElement( Scope scope )
    {
    return graph.getEdgeTarget( scope );
    }

  public String getSourceName( Tap source )
    {
    return sources.get( source );
    }

  public Collection<Operation> getAllOperations()
    {
    Set<FlowElement> vertices = graph.vertexSet();
    Set<Operation> operations = new HashSet<Operation>();

    for( FlowElement vertice : vertices )
      {
      if( vertice instanceof Operator )
        operations.add( ( (Operator) vertice ).getOperation() );
      }

    return operations;
    }

  /**
   * Method clean removes any temporary files used by this FlowStep instance. It will log any IOExceptions thrown.
   *
   * @param jobConf of type JobConf
   */
  public void clean( JobConf jobConf )
    {
    if( tempSink != null )
      {
      try
        {
        tempSink.deletePath( jobConf );
        }
      catch( Exception exception )
        {
        // sink all exceptions, don't fail app
        logWarn( "unable to remove temporary file: " + tempSink, exception );
        }
      }

    if( sink instanceof TempHfs )
      {
      try
        {
        sink.deletePath( jobConf );
        }
      catch( Exception exception )
        {
        // sink all exceptions, don't fail app
        logWarn( "unable to remove temporary file: " + sink, exception );
        }
      }
    else
      {
      cleanTap( jobConf, sink );
      }

    for( Tap tap : mapperTraps.values() )
      cleanTap( jobConf, tap );

    for( Tap tap : reducerTraps.values() )
      cleanTap( jobConf, tap );

    }

  private void cleanTap( JobConf jobConf, Tap tap )
    {
    try
      {
      Hadoop18TapUtil.cleanupTap( jobConf, tap );
      }
    catch( IOException exception )
      {
      // ignore exception
      }
    }

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

  public FlowStepJob getFlowStepJob( JobConf parentConf ) throws IOException
    {
    return new FlowStepJob( this.getName(), getJobConf( parentConf ) );
    }

  public class FlowStepJob implements Callable<Throwable>
    {
    private final String stepName;
    private JobConf currentConf;
    private JobClient currentJobClient;
    private RunningJob runningJob;

    private List<FlowStepJob> predecessors;
    private final CountDownLatch latch = new CountDownLatch( 1 );
    private boolean stop = false;
    private long pollingInterval;

    public FlowStepJob( String stepName, JobConf currentConf )
      {
      this.stepName = stepName;
      this.currentConf = currentConf;
      this.pollingInterval = Flow.getJobPollingInterval( currentConf );
      }

    public void stop()
      {
      if( LOG.isInfoEnabled() )
        logInfo( "stopping: " + stepName );

      stop = true;

      try
        {
        if( runningJob != null )
          runningJob.killJob();
        }
      catch( IOException exception )
        {
        logWarn( "unable to kill job: " + stepName, exception );
        }
      }

    public void setPredecessors( List<FlowStepJob> predecessors ) throws IOException
      {
      this.predecessors = predecessors;
      }

    public Throwable call()
      {
      try
        {
        for( FlowStepJob predecessor : predecessors )
          {
          if( !predecessor.isSuccessful() )
            {
            logWarn( "abandoning step: " + stepName + ", predecessor failed: " + predecessor.stepName );

            return null;
            }
          }

        if( stop )
          return null;

        if( LOG.isInfoEnabled() )
          logInfo( "starting step: " + stepName );

        currentJobClient = new JobClient( currentConf );
        runningJob = currentJobClient.submitJob( currentConf );

        blockTillCompleteOrStopped();

        if( !stop && !runningJob.isSuccessful() )
          {
          dumpCompletionEvents();

          return new FlowException( "step failed: " + stepName );
          }
        }
      catch( Throwable throwable )
        {
        dumpCompletionEvents();
        return throwable;
        }
      finally
        {
        latch.countDown();
        }

      return null;
      }

    protected void blockTillCompleteOrStopped() throws IOException
      {
      while( true )
        {
        if( stop || runningJob.isComplete() )
          break;

        sleep();
        }
      }

    protected void sleep()
      {
      try
        {
        Thread.sleep( pollingInterval );
        }
      catch( InterruptedException exception )
        {
        // do nothing
        }
      }

    private void dumpCompletionEvents()
      {
      try
        {
        if( runningJob == null )
          return;

        TaskCompletionEvent[] events = runningJob.getTaskCompletionEvents( 0 );
        logWarn( "completion events count: " + events.length );

        for( TaskCompletionEvent event : events )
          logWarn( "event = " + event );
        }
      catch( IOException exception )
        {
        logError( "failed reading completion events", exception );
        }
      }

    /**
     * Method isSuccessful returns true if this step completed successfully.
     *
     * @return the successful (type boolean) of this FlowStepJob object.
     */
    public boolean isSuccessful()
      {
      try
        {
        latch.await();

        return runningJob != null && runningJob.isSuccessful();
        }
      catch( InterruptedException exception )
        {
        logWarn( "latch interrupted", exception );
        }
      catch( IOException exception )
        {
        logWarn( "error querying job", exception );
        }

      return false;
      }

    /**
     * Method wasStarted returns true if this job was started
     *
     * @return boolean
     */
    public boolean wasStarted()
      {
      return runningJob != null;
      }
    }

  private void logInfo( String message )
    {
    LOG.info( "[" + Util.truncate( getParentFlowName(), 25 ) + "] " + message );
    }

  private void logWarn( String message )
    {
    LOG.warn( "[" + Util.truncate( getParentFlowName(), 25 ) + "] " + message );
    }

  private void logWarn( String message, Throwable throwable )
    {
    LOG.warn( "[" + Util.truncate( getParentFlowName(), 25 ) + "] " + message, throwable );
    }

  private void logError( String message, Throwable throwable )
    {
    LOG.error( "[" + Util.truncate( getParentFlowName(), 25 ) + "] " + message, throwable );
    }
  }
