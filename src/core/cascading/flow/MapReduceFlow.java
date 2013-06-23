/*
 * Copyright (c) 2007-2010 Concurrent, Inc. All Rights Reserved.
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

import java.beans.ConstructorProperties;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import cascading.scheme.Scheme;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.log4j.Logger;

/**
 * Class MapReduceFlow is a {@link Flow} subclass that supports custom MapReduce jobs pre-configured via the {@link JobConf}
 * object.
 * <p/>
 * Use this class to allow custom MapReduce jobs to participate in the {@link cascading.cascade.Cascade} scheduler. If
 * other Flow instances in the Cascade share resources with this Flow instance, all participants will be scheduled
 * according to their dependencies (topologically).
 * <p/>
 * Set the parameter {@code deleteSinkOnInit} to {@code true} if the outputPath in the jobConf should be deleted before executing the MapReduce job.
 */
public class MapReduceFlow extends Flow
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( MapReduceFlow.class );

  /** Field deleteSinkOnInit */
  private boolean deleteSinkOnInit = false;

  /**
   * Constructor MapReduceFlow creates a new MapReduceFlow instance.
   *
   * @param jobConf of type JobConf
   */
  @ConstructorProperties({"jobConf"})
  public MapReduceFlow( JobConf jobConf )
    {
    this( jobConf.getJobName(), jobConf, false );
    }

  /**
   * Constructor MapReduceFlow creates a new MapReduceFlow instance.
   *
   * @param jobConf          of type JobConf
   * @param deleteSinkOnInit of type boolean
   */
  @ConstructorProperties({"jobConf", "deleteSinkOnInit"})
  public MapReduceFlow( JobConf jobConf, boolean deleteSinkOnInit )
    {
    this( jobConf.getJobName(), jobConf, deleteSinkOnInit );
    }

  /**
   * Constructor MapReduceFlow creates a new MapReduceFlow instance.
   *
   * @param name    of type String
   * @param jobConf of type JobConf
   */
  @ConstructorProperties({"name", "jobConf"})
  public MapReduceFlow( String name, JobConf jobConf )
    {
    this( name, jobConf, false );
    }

  /**
   * Constructor MapReduceFlow creates a new MapReduceFlow instance.
   *
   * @param name             of type String
   * @param jobConf          of type JobConf
   * @param deleteSinkOnInit of type boolean
   */
  @ConstructorProperties({"name", "jobConf", "deleteSinkOnInit"})
  public MapReduceFlow( String name, JobConf jobConf, boolean deleteSinkOnInit )
    {
    this( name, jobConf, deleteSinkOnInit, true );
    }

  /**
   * Constructor MapReduceFlow creates a new MapReduceFlow instance.
   *
   * @param name             of type String
   * @param jobConf          of type JobConf
   * @param deleteSinkOnInit of type boolean
   * @param stopJobsOnExit   of type boolean
   */
  @ConstructorProperties({"name", "jobConf", "deleteSinkOnInit", "stopJobsOnExit"})
  public MapReduceFlow( String name, JobConf jobConf, boolean deleteSinkOnInit, boolean stopJobsOnExit )
    {
    this.deleteSinkOnInit = deleteSinkOnInit;
    this.stopJobsOnExit = stopJobsOnExit;

    setName( name );
    setSources( createSources( jobConf ) );
    setSinks( createSinks( jobConf ) );
    setTraps( createTraps( jobConf ) );
    setStepGraph( makeStepGraph( jobConf ) );
    }

  /**
   * Constructor MapReduceFlow creates a new MapReduceFlow instance.
   *
   * @param name             of type String
   * @param jobConf          of type JobConf
   * @param source           of type Tap
   * @param sink             of type Tap
   * @param deleteSinkOnInit of type boolean
   * @param stopJobsOnExit   of type boolean
   */
  @ConstructorProperties({"name", "jobConf", "source", "sink", "deleteSinkOnInit", "stopJobsOnExit"})
  public MapReduceFlow( String name, JobConf jobConf, Tap source, Tap sink, boolean deleteSinkOnInit, boolean stopJobsOnExit )
    {
      this( name, jobConf, null, null, null, deleteSinkOnInit, stopJobsOnExit );

      Map<String, Tap> sources = new HashMap<String, Tap>();
      Map<String, Tap> sinks   = new HashMap<String, Tap>();
      Map<String, Tap> traps   = new HashMap<String, Tap>();

      sources.put( source.getPath().toString(), source );
      sinks.put( sink.getPath().toString(), sink );

      setSources( sources );
      setSinks( sinks );
      setTraps( traps );
      setStepGraph( makeStepGraph( jobConf ) );
    }

  /**
   * Constructor MapReduceFlow creates a new MapReduceFlow instance.
   *
   * @param name             of type String
   * @param jobConf          of type JobConf
   * @param sources          of type Map<String, Tap>
   * @param sinks            of type Map<String, Tap>
   * @param traps            of type Map<String, Tap>
   * @param deleteSinkOnInit of type boolean
   * @param stopJobsOnExit   of type boolean
   */
  @ConstructorProperties({"name", "jobConf", "sources", "sinks", "traps", "deleteSinkOnInit", "stopJobsOnExit"})
  public MapReduceFlow( String name, JobConf jobConf, Map<String, Tap> sources, Map<String, Tap> sinks, Map<String, Tap> traps, 
      boolean deleteSinkOnInit, boolean stopJobsOnExit )
    {
    this.deleteSinkOnInit = deleteSinkOnInit;
    this.stopJobsOnExit = stopJobsOnExit;

    setName( name );

    if( sources != null )
      setSources( sources );

    if( sinks != null )
      setSinks( sinks );

    if( traps != null )
      setTraps( traps );

    if( (sources != null) && (sinks != null) && (traps != null) )
      setStepGraph( makeStepGraph( jobConf ) );
    }

  private StepGraph makeStepGraph( JobConf jobConf )
    {
    StepGraph stepGraph = new StepGraph();

    Tap sink = getSinksCollection().iterator().next();
    FlowStep step = new MapReduceFlowStep( sink.toString(), jobConf, sink );

    step.setParentFlowName( getName() );

    stepGraph.addVertex( step );

    return stepGraph;
    }

  private Map<String, Tap> createSources( JobConf jobConf )
    {
    Path[] paths = FileInputFormat.getInputPaths( jobConf );

    Map<String, Tap> taps = new HashMap<String, Tap>();

    for( Path path : paths )
      taps.put( path.toString(), new Hfs( new NullScheme(), path.toString() ) );

    return taps;
    }

  private Map<String, Tap> createSinks( JobConf jobConf )
    {
    Map<String, Tap> taps = new HashMap<String, Tap>();

    String path = FileOutputFormat.getOutputPath( jobConf ).toString();

    taps.put( path, new Hfs( new NullScheme(), path, deleteSinkOnInit ) );

    return taps;
    }

  private Map<String, Tap> createTraps( JobConf jobConf )
    {
    return new HashMap<String, Tap>();
    }

  class NullScheme extends Scheme
    {
    public void sourceInit( Tap tap, JobConf conf ) throws IOException
      {
      }

    public void sinkInit( Tap tap, JobConf conf ) throws IOException
      {
      }

    public Tuple source( Object key, Object value )
      {
      if( value instanceof Comparable )
        return new Tuple( (Comparable) key, (Comparable) value );
      else
        return new Tuple( (Comparable) key );
      }

    @Override
    public String toString()
      {
      return getClass().getSimpleName();
      }

    public void sink( TupleEntry tupleEntry, OutputCollector outputCollector ) throws IOException
      {
      throw new UnsupportedOperationException( "sinking is not supported in the scheme" );
      }
    }
  }
