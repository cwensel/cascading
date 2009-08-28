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
import java.util.HashMap;
import java.util.Map;

import cascading.scheme.Scheme;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

/**
 * Class MapReduceFlow is a {@link Flow} subclass that supports custom MapReduce jobs preconfigured via the {@link Job}
 * object.
 * <p/>
 * Use this class to allow custom MapReduce jobs to participage in the {@link cascading.cascade.Cascade} scheduler. If
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
   * @param job
   */
  public MapReduceFlow( Job job )
    {
    this( job.getJobName(), job, false );
    }

  /**
   * Constructor MapReduceFlow creates a new MapReduceFlow instance.
   *
   * @param job
   * @param deleteSinkOnInit of type boolean
   */
  public MapReduceFlow( Job job, boolean deleteSinkOnInit )
    {
    this( job.getJobName(), job, deleteSinkOnInit );
    }

  /**
   * Constructor MapReduceFlow creates a new MapReduceFlow instance.
   *
   * @param name of type String
   * @param job
   */
  public MapReduceFlow( String name, Job job )
    {
    this( name, job, false );
    }

  /**
   * Constructor MapReduceFlow creates a new MapReduceFlow instance.
   *
   * @param name             of type String
   * @param job
   * @param deleteSinkOnInit of type boolean
   */
  public MapReduceFlow( String name, Job job, boolean deleteSinkOnInit )
    {
    this( name, job, deleteSinkOnInit, true );
    }

  /**
   * Constructor MapReduceFlow creates a new MapReduceFlow instance.
   *
   * @param name             of type String
   * @param job
   * @param deleteSinkOnInit of type boolean
   * @param stopJobsOnExit   of type boolean
   */
  public MapReduceFlow( String name, Job job, boolean deleteSinkOnInit, boolean stopJobsOnExit )
    {
    this.deleteSinkOnInit = deleteSinkOnInit;
    this.stopJobsOnExit = stopJobsOnExit;

    setName( name );
    setSources( createSources( job ) );
    setSinks( createSinks( job ) );
    setTraps( createTraps( job ) );
    setStepGraph( makeStepGraph( job ) );
    }

  private StepGraph makeStepGraph( Job job )
    {
    StepGraph stepGraph = new StepGraph();

    Tap sink = getSinks().values().iterator().next();
    FlowStep step = new MapReduceFlowStep( sink.toString(), job, sink );

    step.setParentFlowName( getName() );

    stepGraph.addVertex( step );

    return stepGraph;
    }

  private Map<String, Tap> createSources( Job job )
    {
    Path[] paths = FileInputFormat.getInputPaths( job );

    Map<String, Tap> taps = new HashMap<String, Tap>();

    for( Path path : paths )
      taps.put( path.toString(), new Hfs( new NullScheme(), path.toString() ) );

    return taps;
    }

  private Map<String, Tap> createSinks( Job job )
    {
    Map<String, Tap> taps = new HashMap<String, Tap>();

    String path = FileOutputFormat.getOutputPath( job ).toString();

    taps.put( path, new Hfs( new NullScheme(), path, deleteSinkOnInit ) );

    return taps;
    }

  private Map<String, Tap> createTraps( Job job )
    {
    return new HashMap<String, Tap>();
    }

  class NullScheme extends Scheme
    {
    public void sourceInit( Tap tap, Job job ) throws IOException
      {
      }

    public void sinkInit( Tap tap, Job job ) throws IOException
      {
      }

    public void source( Tuple tuple, TupleEntryCollector tupleEntryCollector )
      {
      tupleEntryCollector.add( tuple );
      }

    @Override
    public String toString()
      {
      return getClass().getSimpleName();
      }

    public void sink( TupleEntry tupleEntry, TupleEntryCollector tupleEntryCollector ) throws IOException
      {
      throw new UnsupportedOperationException( "sinking is not supported in the scheme" );
      }
    }
  }
