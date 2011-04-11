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

package cascading.flow.hadoop;

import java.beans.ConstructorProperties;
import java.util.HashMap;
import java.util.Map;

import cascading.flow.planner.FlowStep;
import cascading.flow.planner.StepGraph;
import cascading.scheme.NullScheme;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

/**
 * Class MapReduceFlow is a {@link cascading.flow.hadoop.HadoopFlow} subclass that supports custom MapReduce jobs pre-configured via the {@link JobConf}
 * object.
 * <p/>
 * Use this class to allow custom MapReduce jobs to participate in the {@link cascading.cascade.Cascade} scheduler. If
 * other Flow instances in the Cascade share resources with this Flow instance, all participants will be scheduled
 * according to their dependencies (topologically).
 * <p/>
 * Set the parameter {@code deleteSinkOnInit} to {@code true} if the outputPath in the jobConf should be deleted before executing the MapReduce job.
 */
public class MapReduceFlow extends HadoopFlow
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
    super( null, jobConf, name );
    this.deleteSinkOnInit = deleteSinkOnInit;
    this.stopJobsOnExit = stopJobsOnExit;

    setSources( createSources( jobConf ) );
    setSinks( createSinks( jobConf ) );
    setTraps( createTraps( jobConf ) );
    setStepGraph( makeStepGraph( jobConf ) );
    }

  private StepGraph makeStepGraph( JobConf jobConf )
    {
    StepGraph stepGraph = new HadoopStepGraph();

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

  }
