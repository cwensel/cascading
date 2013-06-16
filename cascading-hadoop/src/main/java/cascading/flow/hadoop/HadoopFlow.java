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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import static cascading.flow.FlowProps.MAX_CONCURRENT_STEPS;
import static cascading.flow.FlowProps.PRESERVE_TEMPORARY_FILES;

/**
 * Class HadoopFlow is the Apache Hadoop specific implementation of a {@link Flow}.
 * <p/>
 * HadoopFlow must be created through a {@link HadoopFlowConnector} instance.
 * <p/>
 * If classpath paths are provided on the {@link FlowDef}, the Hadoop distributed cache mechanism will be used
 * to augment the remote classpath.
 * <p/>
 * Any path elements that are relative will be uploaded to HDFS, and the HDFS URI will be used on the JobConf. Note
 * all paths are added as "files" to the JobConf, not archives, so they aren't needlessly uncompressed cluster side.
 *
 * @see HadoopFlowConnector
 */
public class HadoopFlow extends BaseFlow<JobConf>
  {
  /** Field hdfsShutdown */
  private static Thread hdfsShutdown = null;
  /** Field shutdownHook */
  private static ShutdownUtil.Hook shutdownHook;
  /** Field jobConf */
  private transient JobConf jobConf;
  /** Field preserveTemporaryFiles */
  private boolean preserveTemporaryFiles = false;
  /** Field syncPaths */
  private transient Map<Path, Path> syncPaths;

  protected HadoopFlow()
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

  static int getMaxConcurrentSteps( JobConf jobConf )
    {
    return jobConf.getInt( MAX_CONCURRENT_STEPS, 0 );
    }

  protected HadoopFlow( PlatformInfo platformInfo, Map<Object, Object> properties, JobConf jobConf, String name )
    {
    super( platformInfo, properties, jobConf, name );
    initFromProperties( properties );
    }

  public HadoopFlow( PlatformInfo platformInfo, Map<Object, Object> properties, JobConf jobConf, FlowDef flowDef )
    {
    super( platformInfo, properties, jobConf, flowDef );

    initFromProperties( properties );
    }

  @Override
  protected void initFromProperties( Map<Object, Object> properties )
    {
    super.initFromProperties( properties );
    preserveTemporaryFiles = getPreserveTemporaryFiles( properties );
    }

  protected void initConfig( Map<Object, Object> properties, JobConf parentConfig )
    {
    if( properties != null )
      parentConfig = createConfig( properties, parentConfig );

    if( parentConfig == null ) // this is ok, getJobConf will pass a default parent in
      return;

    jobConf = new JobConf( parentConfig ); // prevent local values from being shared
    jobConf.set( "fs.http.impl", HttpFileSystem.class.getName() );
    jobConf.set( "fs.https.impl", HttpFileSystem.class.getName() );

    syncPaths = HadoopUtil.addToClassPath( jobConf, getClassPath() );
    }

  @Override
  protected void setConfigProperty( JobConf config, Object key, Object value )
    {
    // don't let these objects pass, even though toString is called below.
    if( value instanceof Class || value instanceof JobConf )
      return;

    config.set( key.toString(), value.toString() );
    }

  @Override
  protected JobConf newConfig( JobConf defaultConfig )
    {
    return defaultConfig == null ? new JobConf() : new JobConf( defaultConfig );
    }

  @Override
  public JobConf getConfig()
    {
    if( jobConf == null )
      initConfig( null, new JobConf() );

    return jobConf;
    }

  @Override
  public JobConf getConfigCopy()
    {
    return new JobConf( getConfig() );
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
  public FlowProcess<JobConf> getFlowProcess()
    {
    return new HadoopFlowProcess( getFlowSession(), getConfig() );
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
      copyToDistributedCache();
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

  private void copyToDistributedCache()
    {
    HadoopUtil.syncPaths( jobConf, syncPaths );
    }

  @Override
  public boolean stepsAreLocal()
    {
    return getConfig().get( "mapred.job.tracker" ).equalsIgnoreCase( "local" );
    }

  private void cleanTemporaryFiles( boolean stop )
    {
    if( stop ) // unstable to call fs operations during shutdown
      return;

    // use step config so cascading.flow.step.path property is properly used
    for( FlowStep<JobConf> step : getFlowSteps() )
      ( (BaseFlowStep<JobConf>) step ).clean();
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
  }
