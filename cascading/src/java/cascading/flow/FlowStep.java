/*
 * Copyright (c) 2007-2008 Vinculum Technologies, Inc. All Rights Reserved.
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import cascading.pipe.Group;
import cascading.tap.Tap;
import cascading.tap.TapIterator;
import cascading.tap.TempDfs;
import cascading.tuple.Tuple;
import cascading.util.Util;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.log4j.Logger;
import org.jgrapht.graph.SimpleDirectedGraph;

/** Class FlowStep ... */
public class FlowStep implements Serializable
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( FlowStep.class );

  /** Field name */
  final String name;
  /** Field graph */
  final SimpleDirectedGraph<FlowElement, Scope> graph = new SimpleDirectedGraph<FlowElement, Scope>( Scope.class );

  /** Field sources */
  final Map<Tap, String> sources = new HashMap<Tap, String>();   // all sources and all sinks must have same scheme
  /** Field sink */
  Tap sink;
  /** Field group */
  Group group;

  FlowStep( String name )
    {
    this.name = name;
    }

  public String getName()
    {
    return name;
    }

  JobConf getJobConf() throws IOException
    {
    return getJobConf( null );
    }

  JobConf getJobConf( JobConf parentConf ) throws IOException
    {
    JobConf conf = parentConf == null ? new JobConf( Flow.class ) : new JobConf( parentConf, Flow.class );

    conf.setJobName( "flow: " + name );

    conf.setOutputKeyClass( Tuple.class );
    conf.setOutputValueClass( Tuple.class );

    conf.setMapperClass( FlowMapper.class );
    conf.setCombinerClass( IdentityReducer.class );
    conf.setReducerClass( FlowReducer.class );

    conf.set( FlowConstants.FLOW_STEP, Util.serializeBase64( this ) );

    for( Tap tap : sources.keySet() )
      tap.sourceInit( conf );

    sink.sinkInit( conf );

    return conf;
    }

  public TapIterator openSourceForRead( JobConf conf ) throws IOException
    {
    JobConf thisConf = new JobConf( conf );
    Tap source = sources.keySet().iterator().next();

    thisConf.setInputPath( source.getPath() );

    return new TapIterator( source.getScheme(), thisConf );
    }

  public TapIterator openSinkForRead( JobConf conf ) throws IOException
    {
    return sink.openForRead( conf );
    }

  public Tap findCurrentSource( JobConf jobConf )
    {
    String currentFile = jobConf.get( "map.input.file" );

    for( Tap source : sources.keySet() )
      {
      if( source.containsFile( jobConf, currentFile ) )
        return source;
      }

    FlowException exception = new FlowException( "could not find source Tap for file: " + currentFile );
    LOG.error( exception );
    throw exception;
    }

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
    if( sink instanceof TempDfs )
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

  public FlowStepJob getFlowStepJob()
    {
    return new FlowStepJob( this );
    }

  public class FlowStepJob implements Callable<Throwable>
    {
    private FlowStep flowStep;
    private JobConf currentConf;
    private JobClient currentJobClient;
    private RunningJob runningJob;
    private List<FlowStepJob> predecessors;

    private CountDownLatch latch = new CountDownLatch( 1 );
    private boolean stop = false;

    public FlowStepJob( FlowStep flowStep )
      {
      this.flowStep = flowStep;
      }

    public void stop()
      {
      if( LOG.isInfoEnabled() )
        LOG.info( "stopping: " + flowStep.getName() );

      stop = true;

      try
        {
        if( runningJob != null )
          runningJob.killJob();
        }
      catch( IOException exception )
        {
        LOG.warn( "unable to kill job: " + flowStep.getName(), exception );
        }
      }

    public void init( JobConf jobConf, List<FlowStepJob> predecessors ) throws IOException
      {
      currentConf = flowStep.getJobConf( jobConf );
      this.predecessors = predecessors;

      if( flowStep.sink.getScheme().getNumSinkParts() != 0 )
        currentConf.setNumReduceTasks( flowStep.sink.getScheme().getNumSinkParts() );
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
          LOG.info( "starting step: " + flowStep.getName() );

        currentJobClient = new JobClient( currentConf );
        runningJob = currentJobClient.submitJob( currentConf );

        runningJob.waitForCompletion();

        if( !runningJob.isSuccessful() )
          {
          TaskCompletionEvent[] events = runningJob.getTaskCompletionEvents( 0 );
          System.out.println( "runningJob.getTaskCompletionEvents(0).length = " + events.length );

          for( TaskCompletionEvent event : events )
            System.out.println( "event = " + event );

          return new FlowException( "step failed: " + flowStep.getName() );
          }
        }
      catch( Throwable throwable )
        {
        return throwable;
        }
      finally
        {
        latch.countDown();
        }

      return null;
      }

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
