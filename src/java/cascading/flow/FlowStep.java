/*
 * Copyright (c) 2007-2008 Concurrent, Inc. All Rights Reserved.
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import cascading.pipe.Group;
import cascading.tap.Tap;
import cascading.tap.TapIterator;
import cascading.tap.TempHfs;
import cascading.tuple.Tuple;
import cascading.tuple.TuplePair;
import cascading.tuple.hadoop.GroupingComparator;
import cascading.tuple.hadoop.GroupingPartitioner;
import cascading.tuple.hadoop.ReverseComparator;
import cascading.util.Util;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.log4j.Logger;
import org.jgrapht.graph.SimpleDirectedGraph;

/** Class FlowStep ... */
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
  /** Field traps */
  public final Map<String, Tap> traps = new HashMap<String, Tap>();
  /** Field tempSink */
  TempHfs tempSink; // used if we need to bypass
  /** Field group */
  public Group group;

  FlowStep( String name )
    {
    this.name = name;
    }

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
    JobConf conf = parentConf == null ? new JobConf( FlowStep.class ) : new JobConf( parentConf, FlowStep.class );

    conf.setJobName( getStepName() );

    conf.setOutputKeyClass( Tuple.class );
    conf.setOutputValueClass( Tuple.class );

    conf.setMapperClass( FlowMapper.class );
    conf.setReducerClass( FlowReducer.class );

    for( Tap tap : sources.keySet() )
      tap.sourceInit( conf );

    if( tempSink != null )
      tempSink.sinkInit( conf );
    else
      sink.sinkInit( conf );

    if( !traps.isEmpty() )
      {
      JobConf tempConf = new JobConf( conf );

      for( Tap tap : traps.values() )
        tap.sourceInit( tempConf );
      }

    if( sink.getScheme().getNumSinkParts() != 0 )
      {
      // if no reducer, set num map tasks to control parts
      if( group != null )
        conf.setNumReduceTasks( sink.getScheme().getNumSinkParts() );
      else
        conf.setNumMapTasks( sink.getScheme().getNumSinkParts() );
      }

    if( group == null )
      {
      conf.setNumReduceTasks( 0 ); // disable reducers
      }
    else if( group.isSorted() )
      {
      conf.setPartitionerClass( GroupingPartitioner.class );
      conf.setMapOutputKeyClass( TuplePair.class );

      if( group.isSortReversed() )
        conf.setOutputKeyComparatorClass( ReverseComparator.class );

      // no need to supply a reverse comparator, only equality is checked
      conf.setOutputValueGroupingComparator( GroupingComparator.class );
      }

    // perform last so init above will pass to tasks
    conf.set( FlowConstants.FLOW_STEP, Util.serializeBase64( this ) );

    return conf;
    }

  public TapIterator openSourceForRead( JobConf conf ) throws IOException
    {
    return new TapIterator( sources.keySet().iterator().next(), conf );
    }

  public TapIterator openSinkForRead( JobConf conf ) throws IOException
    {
    return sink.openForRead( conf );
    }

  public Tap findCurrentSource( JobConf jobConf )
    {
    String currentFile = jobConf.get( "map.input.file" );

    if( currentFile == null || currentFile.length() == 0 )
      throw new IllegalStateException( "map.input.file property returned null" );

    // test for the case that multiple taps contain the same file, and fail appropriately 
    List<Tap> found = new ArrayList<Tap>();

    for( Tap source : sources.keySet() )
      {
      if( source.containsFile( jobConf, currentFile ) )
        found.add( source );
      }

    if( found.size() == 1 )
      return found.get( 0 );

    FlowException exception = null;

    if( found.size() > 1 )
      exception = new FlowException( "found more than one source Tap for file: " + currentFile + " taps: " + Util.join( found, "," ) );
    else
      exception = new FlowException( "could not find source Tap for file: " + currentFile );

    LOG.error( exception );
    throw exception;
    }

  public Tap getTrap( String name )
    {
    return traps.get( name );
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
    return graph.outgoingEdgesOf( flowElement ).iterator().next();
    }

  public FlowElement getNextFlowElement( Scope scope )
    {
    return graph.getEdgeTarget( scope );
    }

  public String getSourceName( Tap source )
    {
    return sources.get( source );
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
      catch( IOException exception )
        {
        LOG.warn( "unable to remove temporary file: " + sink, exception );
        }
      }

    if( sink instanceof TempHfs )
      {
      try
        {
        sink.deletePath( jobConf );
        }
      catch( IOException exception )
        {
        LOG.warn( "unable to remove temporary file: " + sink, exception );
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

    public FlowStepJob( String stepName, JobConf currentConf )
      {
      this.stepName = stepName;
      this.currentConf = currentConf;
      }

    public void stop()
      {
      if( LOG.isInfoEnabled() )
        LOG.info( "stopping: " + stepName );

      stop = true;

      try
        {
        if( runningJob != null )
          runningJob.killJob();
        }
      catch( IOException exception )
        {
        LOG.warn( "unable to kill job: " + stepName, exception );
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
            return null;
          }

        if( stop )
          return null;

        if( LOG.isInfoEnabled() )
          LOG.info( "starting step: " + stepName );

        currentJobClient = new JobClient( currentConf );
        runningJob = currentJobClient.submitJob( currentConf );

        runningJob.waitForCompletion();

        if( !runningJob.isSuccessful() )
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

    private void dumpCompletionEvents()
      {
      try
        {
        if( runningJob == null )
          return;

        TaskCompletionEvent[] events = runningJob.getTaskCompletionEvents( 0 );
        LOG.warn( "runningJob.getTaskCompletionEvents(0).length = " + events.length );

        for( TaskCompletionEvent event : events )
          LOG.warn( "event = " + event );
        }
      catch( IOException exception )
        {
        LOG.error( "failed reading completion events", exception );
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
        LOG.warn( "latch interrupted", exception );
        }
      catch( IOException exception )
        {
        LOG.warn( "error querying job", exception );
        }

      return false;
      }

    }

  }
