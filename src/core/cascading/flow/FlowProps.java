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

package cascading.flow;

import java.util.Map;
import java.util.Properties;

import cascading.property.Props;

/**
 * Class FlowProps is a fluent helper class for setting {@link Flow} specific properties through
 * a {@link FlowConnector}.
 *
 * @see cascading.property.AppProps
 * @see cascading.cascade.CascadeProps
 * @see FlowConnectorProps
 */
public class FlowProps extends Props
  {
  public static final String PRESERVE_TEMPORARY_FILES = "cascading.flow.preservetemporaryfiles";
  public static final String JOB_POLLING_INTERVAL = "cascading.flow.job.pollinginterval";
  public static final String MAX_CONCURRENT_STEPS = "cascading.flow.maxconcurrentsteps";
  public static final String STOP_JOBS_ON_EXIT = "cascading.flow.stopjobsonexit"; // create a stop flows on exit for AppConfig

  boolean preserveTemporaryFiles = false;
  int jobPollingInterval = 5000;
  int maxConcurrentSteps = 0;
  boolean stopJobsOnExit = true;

  /**
   * Property preserveTemporaryFiles forces the Flow instance to keep any temporary intermediate data sets. Useful
   * for debugging. Defaults to {@code false}.
   *
   * @param properties             of type Map
   * @param preserveTemporaryFiles of type boolean
   */
  public static void setPreserveTemporaryFiles( Map<Object, Object> properties, boolean preserveTemporaryFiles )
    {
    properties.put( PRESERVE_TEMPORARY_FILES, Boolean.toString( preserveTemporaryFiles ) );
    }

  /**
   * Property jobPollingInterval will set the time to wait between polling the remote server for the status of a job.
   * The default value is 5000 msec (5 seconds).
   *
   * @param properties of type Map
   * @param interval   of type long
   */
  public static void setJobPollingInterval( Map<Object, Object> properties, long interval )
    {
    properties.put( JOB_POLLING_INTERVAL, Long.toString( interval ) );
    }

  /**
   * Method setMaxConcurrentSteps sets the maximum number of steps that a Flow can run concurrently.
   * <p/>
   * By default a Flow will attempt to run all give steps at the same time. But there are occasions
   * where limiting the number of steps helps manages resources.
   *
   * @param properties         of type Map<Object, Object>
   * @param numConcurrentSteps of type int
   */
  public static void setMaxConcurrentSteps( Map<Object, Object> properties, int numConcurrentSteps )
    {
    properties.put( MAX_CONCURRENT_STEPS, Integer.toString( numConcurrentSteps ) );
    }

  /**
   * Property stopJobsOnExit will tell the Flow to add a JVM shutdown hook that will kill all running processes if the
   * underlying computing system supports it. Defaults to {@code true}.
   *
   * @param properties     of type Map
   * @param stopJobsOnExit of type boolean
   */
  public static void setStopJobsOnExit( Map<Object, Object> properties, boolean stopJobsOnExit )
    {
    properties.put( STOP_JOBS_ON_EXIT, Boolean.toString( stopJobsOnExit ) );
    }


  public FlowProps()
    {
    }

  public boolean isPreserveTemporaryFiles()
    {
    return preserveTemporaryFiles;
    }

  public FlowProps setPreserveTemporaryFiles( boolean preserveTemporaryFiles )
    {
    this.preserveTemporaryFiles = preserveTemporaryFiles;

    return this;
    }

  public int getJobPollingInterval()
    {
    return jobPollingInterval;
    }

  public FlowProps setJobPollingInterval( int jobPollingInterval )
    {
    this.jobPollingInterval = jobPollingInterval;

    return this;
    }

  public int getMaxConcurrentSteps()
    {
    return maxConcurrentSteps;
    }

  public FlowProps setMaxConcurrentSteps( int maxConcurrentSteps )
    {
    this.maxConcurrentSteps = maxConcurrentSteps;

    return this;
    }

  public boolean isStopJobsOnExit()
    {
    return stopJobsOnExit;
    }

  public FlowProps setStopJobsOnExit( boolean stopJobsOnExit )
    {
    this.stopJobsOnExit = stopJobsOnExit;

    return this;
    }

  @Override
  protected void addPropertiesTo( Properties properties )
    {
    setPreserveTemporaryFiles( properties, preserveTemporaryFiles );
    setJobPollingInterval( properties, jobPollingInterval );
    setMaxConcurrentSteps( properties, maxConcurrentSteps );
    setStopJobsOnExit( properties, stopJobsOnExit );
    }
  }
