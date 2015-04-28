/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.tez.util;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import cascading.CascadingException;
import cascading.flow.FlowException;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.tap.hadoop.io.MultiInputSplit;
import cascading.util.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.lib.MRReader;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.runtime.api.AbstractLogicalInput;
import org.apache.tez.runtime.api.AbstractLogicalOutput;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.MergedLogicalInput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.flow.hadoop.util.HadoopUtil.getCommonPaths;
import static org.apache.hadoop.yarn.api.ApplicationConstants.CLASS_PATH_SEPARATOR;
import static org.apache.hadoop.yarn.api.ApplicationConstants.Environment.CLASSPATH;
import static org.apache.hadoop.yarn.api.ApplicationConstants.Environment.PWD;
import static org.apache.tez.common.TezUtils.createConfFromByteString;
import static org.apache.tez.common.TezUtils.createConfFromUserPayload;
import static org.apache.tez.mapreduce.hadoop.MRInputHelpers.parseMRInputPayload;

/**
 *
 */
public class TezUtil
  {
  private static final Logger LOG = LoggerFactory.getLogger( TezUtil.class );

  /**
   * Attempting to localize all new JobConf calls
   *
   * @param configuration
   * @return
   */
  public static JobConf asJobConf( Configuration configuration )
    {
    return new JobConf( configuration );
    }

  public static TezConfiguration createTezConf( Map<Object, Object> properties, TezConfiguration defaultJobconf )
    {
    TezConfiguration jobConf = defaultJobconf == null ? new TezConfiguration() : new TezConfiguration( defaultJobconf );

    if( properties == null )
      return jobConf;

    Set<Object> keys = new HashSet<Object>( properties.keySet() );

    // keys will only be grabbed if both key/value are String, so keep orig keys
    if( properties instanceof Properties )
      keys.addAll( ( (Properties) properties ).stringPropertyNames() );

    for( Object key : keys )
      {
      Object value = properties.get( key );

      if( value == null && properties instanceof Properties && key instanceof String )
        value = ( (Properties) properties ).getProperty( (String) key );

      if( value == null ) // don't stuff null values
        continue;

      // don't let these objects pass, even though toString is called below.
      if( value instanceof Class || value instanceof TezConfiguration )
        continue;

      jobConf.set( key.toString(), value.toString() );
      }

    return jobConf;
    }

  public static UserGroupInformation getCurrentUser()
    {
    try
      {
      return UserGroupInformation.getCurrentUser();
      }
    catch( IOException exception )
      {
      throw new CascadingException( "unable to get current user", exception );
      }
    }

  public static String getEdgeSourceID( LogicalInput input, Configuration configuration )
    {
    String id = configuration.get( "cascading.node.source" );

    if( id == null )
      throw new IllegalStateException( "no source id found: " + input.getClass().getName() );

    return id;
    }

  public static String getEdgeSinkID( LogicalOutput output, Configuration configuration )
    {
    String id = configuration.get( "cascading.node.sink" );

    if( id == null )
      throw new IllegalStateException( "no sink id found: " + output.getClass().getName() );

    return id;
    }

  public static Configuration getInputConfiguration( LogicalInput input )
    {
    try
      {
      if( input instanceof MergedLogicalInput )
        input = (LogicalInput) Util.getFirst( ( (MergedLogicalInput) input ).getInputs() );

      if( input instanceof MRInput )
        return createConfFromByteString( parseMRInputPayload( ( (MRInput) input ).getContext().getUserPayload() ).getConfigurationBytes() );

      if( input instanceof AbstractLogicalInput )
        return createConfFromUserPayload( ( (AbstractLogicalInput) input ).getContext().getUserPayload() );
      }
    catch( IOException exception )
      {
      throw new FlowException( "unable to unpack payload", exception );
      }

    throw new IllegalStateException( "unknown input type: " + input.getClass().getName() );
    }

  public static Configuration getOutputConfiguration( LogicalOutput output )
    {
    try
      {
      if( output instanceof MROutput )
        return TezUtils.createConfFromUserPayload( ( (MROutput) output ).getContext().getUserPayload() );

      if( output instanceof AbstractLogicalOutput )
        return createConfFromUserPayload( ( (AbstractLogicalOutput) output ).getContext().getUserPayload() );
      }
    catch( IOException exception )
      {
      throw new FlowException( "unable to unpack payload", exception );
      }

    throw new IllegalStateException( "unknown input type: " + output.getClass().getName() );
    }

  public static void setSourcePathForSplit( MRInput input, MRReader reader, Configuration configuration )
    {
    Path path = null;

    if( Util.returnInstanceFieldIfExistsSafe( input, "useNewApi" ) )
      {
      org.apache.hadoop.mapreduce.InputSplit newInputSplit = (org.apache.hadoop.mapreduce.InputSplit) reader.getSplit();

      if( newInputSplit instanceof org.apache.hadoop.mapreduce.lib.input.FileSplit )
        path = ( (org.apache.hadoop.mapreduce.lib.input.FileSplit) newInputSplit ).getPath();
      }
    else
      {
      org.apache.hadoop.mapred.InputSplit oldInputSplit = (org.apache.hadoop.mapred.InputSplit) reader.getSplit();

      if( oldInputSplit instanceof org.apache.hadoop.mapred.FileSplit )
        path = ( (org.apache.hadoop.mapred.FileSplit) oldInputSplit ).getPath();
      }

    if( path != null )
      configuration.set( MultiInputSplit.CASCADING_SOURCE_PATH, path.toString() );
    }

  public static Map<Path, Path> addToClassPath( Configuration config, String stagingRoot, String resourceSubPath, Collection<String> classpath,
                                                LocalResourceType resourceType, Map<String, LocalResource> localResources,
                                                Map<String, String> environment )
    {
    if( classpath == null )
      return null;

    // given to fully qualified
    Map<String, Path> localPaths = new HashMap<>();
    Map<String, Path> remotePaths = new HashMap<>();

    HadoopUtil.resolvePaths( config, classpath, stagingRoot, resourceSubPath, localPaths, remotePaths );

    try
      {
      LocalFileSystem localFS = HadoopUtil.getLocalFS( config );

      for( String fileName : localPaths.keySet() )
        {
        Path artifact = localPaths.get( fileName );
        Path remotePath = remotePaths.get( fileName );

        if( remotePath == null )
          remotePath = artifact;

        addResource( localResources, environment, fileName, localFS.getFileStatus( artifact ), remotePath, resourceType );
        }

      FileSystem defaultFS = HadoopUtil.getDefaultFS( config );

      for( String fileName : remotePaths.keySet() )
        {
        Path artifact = remotePaths.get( fileName );
        Path localPath = localPaths.get( fileName );

        if( localPath != null )
          continue;

        addResource( localResources, environment, fileName, defaultFS.getFileStatus( artifact ), artifact, resourceType );
        }
      }
    catch( IOException exception )
      {
      throw new FlowException( "unable to set remote resource paths", exception );
      }

    return getCommonPaths( localPaths, remotePaths );
    }

  protected static void addResource( Map<String, LocalResource> localResources, Map<String, String> environment, String fileName, FileStatus stats, Path fullPath, LocalResourceType type ) throws IOException
    {
    if( localResources.containsKey( fileName ) )
      throw new FlowException( "duplicate filename added to classpath resources: " + fileName );

    URL yarnUrlFromPath = ConverterUtils.getYarnUrlFromPath( fullPath );
    long len = stats.getLen();
    long modificationTime = stats.getModificationTime();

    LocalResource resource = LocalResource.newInstance(
      yarnUrlFromPath,
      type,
      LocalResourceVisibility.APPLICATION,
      len,
      modificationTime );

    if( type == LocalResourceType.PATTERN )
      {
      // todo: parametrize this for dynamic inclusion below
      String pattern = "(?:classes/|lib/).*";

      resource.setPattern( pattern );

      if( environment != null )
        {
        String current = "";

        current += PWD.$$() + File.separator + fileName + File.separator + "*" + CLASS_PATH_SEPARATOR;
        current += PWD.$$() + File.separator + fileName + File.separator + "lib" + File.separator + "*" + CLASS_PATH_SEPARATOR;
        current += PWD.$$() + File.separator + fileName + File.separator + "classes" + File.separator + "*" + CLASS_PATH_SEPARATOR;

        String classPath = environment.get( CLASSPATH.name() );

        if( classPath == null )
          classPath = "";
        else if( !classPath.startsWith( CLASS_PATH_SEPARATOR ) )
          classPath += CLASS_PATH_SEPARATOR;

        classPath += current;

        LOG.info( "adding to cluster side classpath: {} ", classPath );

        environment.put( CLASSPATH.name(), classPath );
        }
      }

    localResources.put( fileName, resource );
    }

  public static void setMRProperties( ProcessorContext context, Configuration config, boolean isMapperOutput )
    {
    TaskAttemptID taskAttemptId = org.apache.tez.mapreduce.hadoop.mapreduce.TaskAttemptContextImpl
      .createMockTaskAttemptID( context.getApplicationId().getClusterTimestamp(),
        context.getTaskVertexIndex(), context.getApplicationId().getId(),
        context.getTaskIndex(), context.getTaskAttemptNumber(), isMapperOutput );

    config.set( JobContext.TASK_ATTEMPT_ID, taskAttemptId.toString() );
    config.set( JobContext.TASK_ID, taskAttemptId.getTaskID().toString() );
    config.setBoolean( JobContext.TASK_ISMAP, isMapperOutput );
    config.setInt( JobContext.TASK_PARTITION, taskAttemptId.getTaskID().getId() );
    }
  }
