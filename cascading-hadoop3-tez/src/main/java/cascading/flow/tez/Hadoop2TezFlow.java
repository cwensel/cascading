/*
 * Copyright (c) 2016-2017 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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

package cascading.flow.tez;

import java.io.IOException;
import java.util.Map;

import cascading.flow.BaseFlow;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.FlowException;
import cascading.flow.FlowProcess;
import cascading.flow.FlowStep;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.flow.planner.BaseFlowStep;
import cascading.flow.planner.PlatformInfo;
import cascading.property.PropertyUtil;
import cascading.tap.hadoop.io.HttpFileSystem;
import cascading.util.ShutdownUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.dag.api.TezConfiguration;
import riffle.process.ProcessConfiguration;

import static cascading.flow.FlowProps.MAX_CONCURRENT_STEPS;
import static cascading.flow.FlowProps.PRESERVE_TEMPORARY_FILES;

/**
 * Class HadoopFlow is the Apache Hadoop specific implementation of a {@link cascading.flow.Flow}.
 * <p>
 * HadoopFlow must be created through a {@link cascading.flow.tez.Hadoop2TezFlowConnector} instance.
 * <p>
 * If classpath paths are provided on the {@link cascading.flow.FlowDef}, the Hadoop distributed cache mechanism will be used
 * to augment the remote classpath.
 * <p>
 * Any path elements that are relative will be uploaded to HDFS, and the HDFS URI will be used on the JobConf. Note
 * all paths are added as "files" to the JobConf, not archives, so they aren't needlessly uncompressed cluster side.
 *
 * @see cascading.flow.tez.Hadoop2TezFlowConnector
 */
public class Hadoop2TezFlow extends BaseFlow<TezConfiguration>
  {
  /** Field hdfsShutdown */
  private static Thread hdfsShutdown = null;
  /** Field shutdownHook */
  private static ShutdownUtil.Hook shutdownHook;
  /** Field jobConf */
  private transient TezConfiguration flowConf;
  /** Field preserveTemporaryFiles */
  private boolean preserveTemporaryFiles = false;

  private String flowStagingPath;

  protected Hadoop2TezFlow()
    {
    }

  /**
   * Returns property preserveTemporaryFiles.
   *
   * @param properties of type Map
   * @return a boolean
   */
  static boolean getPreserveTemporaryFiles( Map<Object, Object> properties )
    {
    return Boolean.parseBoolean( PropertyUtil.getProperty( properties, PRESERVE_TEMPORARY_FILES, "false" ) );
    }

  static int getMaxConcurrentSteps( TezConfiguration jobConf )
    {
    return jobConf.getInt( MAX_CONCURRENT_STEPS, 0 );
    }

  public Hadoop2TezFlow( PlatformInfo platformInfo, Map<Object, Object> properties, TezConfiguration flowConf, FlowDef flowDef )
    {
    super( platformInfo, properties, flowConf, flowDef );

    initFromProperties( properties );
    }

  @Override
  protected void initFromProperties( Map<Object, Object> properties )
    {
    super.initFromProperties( properties );

    preserveTemporaryFiles = getPreserveTemporaryFiles( properties );
    }

  protected void initConfig( Map<Object, Object> properties, TezConfiguration parentConfig )
    {
    if( properties != null )
      parentConfig = createConfig( properties, parentConfig );

    if( parentConfig == null ) // this is ok, getJobConf will pass a default parent in
      return;

    flowConf = new TezConfiguration( parentConfig ); // prevent local values from being shared
    flowConf.set( "fs.http.impl", HttpFileSystem.class.getName() );
    flowConf.set( "fs.https.impl", HttpFileSystem.class.getName() );

    UserGroupInformation.setConfiguration( flowConf );

    flowStagingPath = createStagingRoot();
    }

  public String getFlowStagingPath()
    {
    if( flowStagingPath == null )
      flowStagingPath = createStagingRoot();

    return flowStagingPath;
    }

  private String createStagingRoot()
    {
    return ".staging" + Path.SEPARATOR + getID();
    }

  @Override
  protected void setConfigProperty( TezConfiguration config, Object key, Object value )
    {
    // don't let these objects pass, even though toString is called below.
    if( value instanceof Class || value instanceof Configuration || value == null )
      return;

    config.set( key.toString(), value.toString() );
    }

  @Override
  protected TezConfiguration newConfig( TezConfiguration defaultConfig )
    {
    return defaultConfig == null ? new TezConfiguration() : new TezConfiguration( defaultConfig );
    }

  @ProcessConfiguration
  @Override
  public TezConfiguration getConfig()
    {
    if( flowConf == null )
      initConfig( null, new TezConfiguration() );

    return flowConf;
    }

  @Override
  public TezConfiguration getConfigCopy()
    {
    return new TezConfiguration( getConfig() );
    }

  @Override
  public Map<Object, Object> getConfigAsProperties()
    {
    return HadoopUtil.createProperties( getConfig() );
    }

  /**
   * Method getProperty returns the value associated with the given key from the underlying properties system.
   *
   * @param key of type String
   * @return String
   */
  public String getProperty( String key )
    {
    return getConfig().get( key );
    }

  @Override
  public FlowProcess<TezConfiguration> getFlowProcess()
    {
    return new Hadoop2TezFlowProcess( getFlowSession(), null, getConfig() );
    }

  /**
   * Method isPreserveTemporaryFiles returns false if temporary files will be cleaned when this Flow completes.
   *
   * @return the preserveTemporaryFiles (type boolean) of this Flow object.
   */
  public boolean isPreserveTemporaryFiles()
    {
    return preserveTemporaryFiles;
    }

  @Override
  protected void internalStart()
    {
    try
      {
      copyArtifactsToRemote();
      deleteSinksIfReplace();
      deleteTrapsIfReplace();
      deleteCheckpointsIfReplace();
      }
    catch( IOException exception )
      {
      throw new FlowException( "unable to delete sinks", exception );
      }

    registerHadoopShutdownHook( this );
    }

  private void copyArtifactsToRemote()
    {
    for( FlowStep<TezConfiguration> flowStep : getFlowSteps() )
      ( (Hadoop2TezFlowStep) flowStep ).syncArtifacts();
    }

  @Override
  public boolean stepsAreLocal()
    {
    return HadoopUtil.isLocal( getConfig() );
    }

  private void cleanTemporaryFiles( boolean stop )
    {
    if( stop ) // unstable to call fs operations during shutdown
      return;

    // use step config so cascading.flow.step.path property is properly used
    for( FlowStep<TezConfiguration> step : getFlowSteps() )
      ( (BaseFlowStep<TezConfiguration>) step ).clean();
    }

  private static synchronized void registerHadoopShutdownHook( Flow flow )
    {
    if( !flow.isStopJobsOnExit() )
      return;

    // guaranteed singleton here
    if( shutdownHook != null )
      return;

    getHdfsShutdownHook();

    shutdownHook = new ShutdownUtil.Hook()
      {
      @Override
      public Priority priority()
        {
        return Priority.LAST; // very last thing to happen
        }

      @Override
      public void execute()
        {
        callHdfsShutdownHook();
        }
      };

    ShutdownUtil.addHook( shutdownHook );
    }

  private synchronized static void callHdfsShutdownHook()
    {
    if( hdfsShutdown != null )
      hdfsShutdown.start();
    }

  private synchronized static void getHdfsShutdownHook()
    {
    if( hdfsShutdown == null )
      hdfsShutdown = HadoopUtil.getHDFSShutdownHook();
    }

  protected void internalClean( boolean stop )
    {
    if( !isPreserveTemporaryFiles() )
      cleanTemporaryFiles( stop );
    }

  protected void internalShutdown()
    {
    }

  protected int getMaxNumParallelSteps()
    {
    return stepsAreLocal() ? 1 : getMaxConcurrentSteps( getConfig() );
    }

  @Override
  protected long getTotalSliceCPUMilliSeconds()
    {
    long counterValue = flowStats.getCounterValue( TaskCounter.CPU_MILLISECONDS );

    if( counterValue == 0 )
      return -1;

    return counterValue;
    }
  }
