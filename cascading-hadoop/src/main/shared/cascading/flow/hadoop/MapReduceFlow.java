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

package cascading.flow.hadoop;

import java.beans.ConstructorProperties;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import cascading.flow.FlowStep;
import cascading.flow.hadoop.planner.HadoopStepGraph;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.flow.planner.FlowStepGraph;
import cascading.scheme.NullScheme;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;

/**
 * Class MapReduceFlow is a {@link cascading.flow.hadoop.HadoopFlow} subclass that supports custom MapReduce jobs
 * pre-configured via the {@link JobConf} object.
 * <p/>
 * Use this class to allow custom MapReduce jobs to participate in the {@link cascading.cascade.Cascade} scheduler. If
 * other Flow instances in the Cascade share resources with this Flow instance, all participants will be scheduled
 * according to their dependencies (topologically).
 * <p/>
 * Set the parameter {@code deleteSinkOnInit} to {@code true} if the outputPath in the jobConf should be deleted before executing the MapReduce job.
 * <p/>
 * MapReduceFlow assumes the underlying input and output paths are compatible with the {@link Hfs} Tap.
 * <p/>
 * If the configured JobConf instance uses some other identifier instead of Hadoop FS paths, you should override the
 * {@link #createSources(org.apache.hadoop.mapred.JobConf)}, {@link #createSinks(org.apache.hadoop.mapred.JobConf)}, and
 * {@link #createTraps(org.apache.hadoop.mapred.JobConf)} methods to properly resolve the configured paths into
 * usable {@link Tap} instances. By default createTraps returns an empty collection and should probably be left alone.
 */
public class MapReduceFlow extends HadoopFlow
  {
  /** Field deleteSinkOnInit */
  protected boolean deleteSinkOnInit = false;

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
    super( HadoopUtil.getPlatformInfo(), new Properties(), jobConf, name );
    this.deleteSinkOnInit = deleteSinkOnInit;
    this.stopJobsOnExit = stopJobsOnExit;

    setSources( createSources( jobConf ) );
    setSinks( createSinks( jobConf ) );
    setTraps( createTraps( jobConf ) );
    setFlowStepGraph( makeStepGraph( jobConf ) );
    initSteps();

    initializeNewJobsMap();
    }

  private FlowStepGraph makeStepGraph( JobConf jobConf )
    {
    FlowStepGraph<JobConf> flowStepGraph = new HadoopStepGraph();

    Tap sink = getSinksCollection().iterator().next();
    FlowStep<JobConf> step = new MapReduceFlowStep( getName(), sink.toString(), jobConf, sink );

    flowStepGraph.addVertex( step );

    return flowStepGraph;
    }

  protected Map<String, Tap> createSources( JobConf jobConf )
    {
    Path[] paths = FileInputFormat.getInputPaths( jobConf );

    Map<String, Tap> taps = new HashMap<String, Tap>();

    for( Path path : paths )
      taps.put( path.toString(), new Hfs( new NullScheme(), path.toString() ) );

    return taps;
    }

  protected Map<String, Tap> createSinks( JobConf jobConf )
    {
    Map<String, Tap> taps = new HashMap<String, Tap>();

    String path = FileOutputFormat.getOutputPath( jobConf ).toString();

    taps.put( path, new Hfs( new NullScheme(), path, deleteSinkOnInit ) );

    return taps;
    }

  protected Map<String, Tap> createTraps( JobConf jobConf )
    {
    return new HashMap<String, Tap>();
    }
  }
