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

package cascading.flow.hadoop.util;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import cascading.CascadingException;
import cascading.flow.FlowException;
import cascading.flow.planner.BaseFlowStep;
import cascading.flow.planner.PlatformInfo;
import cascading.flow.planner.Scope;
import cascading.pipe.Group;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.util.LogUtil;
import cascading.util.Util;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.util.Util.invokeInstanceMethod;

/**
 *
 */
public class HadoopUtil
  {
  public static final String CASCADING_FLOW_EXECUTING = "cascading.flow.executing";

  private static final Logger LOG = LoggerFactory.getLogger( HadoopUtil.class );
  private static final String ENCODING = "US-ASCII";
  private static final Class<?> DEFAULT_OBJECT_SERIALIZER = JavaObjectSerializer.class;

  private static PlatformInfo platformInfo;

  public static void setIsInflow( Configuration conf )
    {
    conf.setBoolean( CASCADING_FLOW_EXECUTING, true );
    }

  public static boolean isInflow( Configuration conf )
    {
    return conf.getBoolean( CASCADING_FLOW_EXECUTING, false );
    }

  public static void initLog4j( JobConf configuration )
    {
    initLog4j( (Configuration) configuration );
    }

  public static void initLog4j( Configuration configuration )
    {
    String values = configuration.get( "log4j.logger", null );

    if( values == null || values.length() == 0 )
      return;

    if( !Util.hasClass( "org.apache.log4j.Logger" ) )
      {
      LOG.info( "org.apache.log4j.Logger is not in the current CLASSPATH, not setting log4j.logger properties" );
      return;
      }

    String[] elements = values.split( "," );

    for( String element : elements )
      LogUtil.setLog4jLevel( element.split( "=" ) );
    }

  // only place JobConf should ever be returned
  public static JobConf asJobConfInstance( Configuration configuration )
    {
    if( configuration instanceof JobConf )
      return (JobConf) configuration;

    return new JobConf( configuration );
    }

  public static <C> C copyJobConf( C parentJobConf )
    {
    return copyConfiguration( parentJobConf );
    }

  public static JobConf copyJobConf( JobConf parentJobConf )
    {
    if( parentJobConf == null )
      throw new IllegalArgumentException( "parent may not be null" );

    // see https://github.com/Cascading/cascading/pull/21
    // The JobConf(JobConf) constructor causes derived JobConfs to share Credentials. We want to avoid this, in
    // case those Credentials are mutated later on down the road (which they will be, during job submission, in
    // separate threads!). Using the JobConf(Configuration) constructor avoids Credentials-sharing.
    final Configuration configurationCopy = new Configuration( parentJobConf );
    final JobConf jobConf = new JobConf( configurationCopy );

    jobConf.getCredentials().addAll( parentJobConf.getCredentials() );

    return jobConf;
    }

  public static JobConf createJobConf( Map<Object, Object> properties, JobConf defaultJobconf )
    {
    JobConf jobConf = defaultJobconf == null ? new JobConf() : copyJobConf( defaultJobconf );

    if( properties == null )
      return jobConf;

    return copyConfiguration( properties, jobConf );
    }

  public static <C> C copyConfiguration( C parent )
    {
    if( parent == null )
      throw new IllegalArgumentException( "parent may not be null" );

    if( !( parent instanceof Configuration ) )
      throw new IllegalArgumentException( "parent must be of type Configuration" );

    Configuration conf = (Configuration) parent;

    // see https://github.com/Cascading/cascading/pull/21
    // The JobConf(JobConf) constructor causes derived JobConfs to share Credentials. We want to avoid this, in
    // case those Credentials are mutated later on down the road (which they will be, during job submission, in
    // separate threads!). Using the JobConf(Configuration) constructor avoids Credentials-sharing.
    Configuration configurationCopy = new Configuration( conf );

    Configuration copiedConf = callCopyConstructor( parent.getClass(), configurationCopy );

    if( Util.hasInstanceMethod( parent, "getCredentials", null ) )
      {
      Object result = invokeInstanceMethod( parent, "getCredentials", null, null );
      Object credentials = invokeInstanceMethod( copiedConf, "getCredentials", null, null );

      invokeInstanceMethod( credentials, "addAll", new Object[]{result}, new Class[]{credentials.getClass()} );
      }

    return (C) copiedConf;
    }

  protected static <C extends Configuration> C callCopyConstructor( Class type, Configuration parent )
    {
    try
      {
      Constructor<C> constructor = type.getConstructor( parent.getClass() );

      return constructor.newInstance( parent );
      }
    catch( NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException exception )
      {
      throw new CascadingException( "unable to create copy of: " + type );
      }
    }

  public static <C extends Configuration> C copyConfiguration( Map<Object, Object> srcProperties, C dstConfiguration )
    {
    Set<Object> keys = new HashSet<Object>( srcProperties.keySet() );

    // keys will only be grabbed if both key/value are String, so keep orig keys
    if( srcProperties instanceof Properties )
      keys.addAll( ( (Properties) srcProperties ).stringPropertyNames() );

    for( Object key : keys )
      {
      Object value = srcProperties.get( key );

      if( value == null && srcProperties instanceof Properties && key instanceof String )
        value = ( (Properties) srcProperties ).getProperty( (String) key );

      if( value == null ) // don't stuff null values
        continue;

      // don't let these objects pass, even though toString is called below.
      if( value instanceof Class || value instanceof JobConf )
        continue;

      dstConfiguration.set( key.toString(), value.toString() );
      }

    return dstConfiguration;
    }

  public static Map<Object, Object> createProperties( Configuration jobConf )
    {
    Map<Object, Object> properties = new HashMap<Object, Object>();

    if( jobConf == null )
      return properties;

    for( Map.Entry<String, String> entry : jobConf )
      properties.put( entry.getKey(), entry.getValue() );

    return properties;
    }

  public static Thread getHDFSShutdownHook()
    {
    Exception caughtException;

    try
      {
      // we must init the FS so the finalizer is registered
      FileSystem.getLocal( new JobConf() );

      Field field = FileSystem.class.getDeclaredField( "clientFinalizer" );
      field.setAccessible( true );

      Thread finalizer = (Thread) field.get( null );

      if( finalizer != null )
        Runtime.getRuntime().removeShutdownHook( finalizer );

      return finalizer;
      }
    catch( NoSuchFieldException exception )
      {
      caughtException = exception;
      }
    catch( IllegalAccessException exception )
      {
      caughtException = exception;
      }
    catch( IOException exception )
      {
      caughtException = exception;
      }

    LOG.debug( "unable to find and remove client hdfs shutdown hook, received exception: {}", caughtException.getClass().getName() );

    return null;
    }

  public static String encodeBytes( byte[] bytes )
    {
    try
      {
      return new String( Base64.encodeBase64( bytes ), ENCODING );
      }
    catch( UnsupportedEncodingException exception )
      {
      throw new RuntimeException( exception );
      }
    }

  public static byte[] decodeBytes( String string )
    {
    try
      {
      byte[] bytes = string.getBytes( ENCODING );
      return Base64.decodeBase64( bytes );
      }
    catch( UnsupportedEncodingException exception )
      {
      throw new RuntimeException( exception );
      }
    }

  public static <T> ObjectSerializer instantiateSerializer( Configuration conf, Class<T> type ) throws ClassNotFoundException
    {
    Class<ObjectSerializer> flowSerializerClass;

    String serializerClassName = conf.get( ObjectSerializer.OBJECT_SERIALIZER_PROPERTY );

    if( serializerClassName == null || serializerClassName.length() == 0 )
      flowSerializerClass = (Class<ObjectSerializer>) DEFAULT_OBJECT_SERIALIZER;
    else
      flowSerializerClass = (Class<ObjectSerializer>) Class.forName( serializerClassName );

    ObjectSerializer objectSerializer;

    try
      {
      objectSerializer = flowSerializerClass.newInstance();

      if( objectSerializer instanceof Configurable )
        ( (Configurable) objectSerializer ).setConf( conf );
      }
    catch( Exception exception )
      {
      exception.printStackTrace();
      throw new IllegalArgumentException( "Unable to instantiate serializer \""
        + flowSerializerClass.getName()
        + "\" for class: "
        + type.getName() );
      }

    if( !objectSerializer.accepts( type ) )
      throw new IllegalArgumentException( serializerClassName + " won't accept objects of class " + type.toString() );

    return objectSerializer;
    }

  public static <T> String serializeBase64( T object, Configuration conf ) throws IOException
    {
    return serializeBase64( object, conf, true );
    }

  public static <T> String serializeBase64( T object, Configuration conf, boolean compress ) throws IOException
    {
    ObjectSerializer objectSerializer;

    try
      {
      objectSerializer = instantiateSerializer( conf, object.getClass() );
      }
    catch( ClassNotFoundException exception )
      {
      throw new IOException( exception );
      }

    return encodeBytes( objectSerializer.serialize( object, compress ) );
    }

  /**
   * This method deserializes the Base64 encoded String into an Object instance.
   *
   * @param string
   * @return an Object
   */
  public static <T> T deserializeBase64( String string, Configuration conf, Class<T> type ) throws IOException
    {
    return deserializeBase64( string, conf, type, true );
    }

  public static <T> T deserializeBase64( String string, Configuration conf, Class<T> type, boolean decompress ) throws IOException
    {
    if( string == null || string.length() == 0 )
      return null;

    ObjectSerializer objectSerializer;

    try
      {
      objectSerializer = instantiateSerializer( conf, type );
      }
    catch( ClassNotFoundException exception )
      {
      throw new IOException( exception );
      }

    return objectSerializer.deserialize( decodeBytes( string ), type, decompress );
    }

  public static Class findMainClass( Class defaultType )
    {
    return Util.findMainClass( defaultType, "org.apache.hadoop" );
    }

  public static Map<String, String> getConfig( Configuration defaultConf, Configuration updatedConf )
    {
    Map<String, String> configs = new HashMap<String, String>();

    for( Map.Entry<String, String> entry : updatedConf )
      configs.put( entry.getKey(), entry.getValue() );

    for( Map.Entry<String, String> entry : defaultConf )
      {
      if( entry.getValue() == null )
        continue;

      String updatedValue = configs.get( entry.getKey() );

      // if both null, lets purge from map to save space
      if( updatedValue == null && entry.getValue() == null )
        configs.remove( entry.getKey() );

      // if the values are the same, lets also purge from map to save space
      if( updatedValue != null && updatedValue.equals( entry.getValue() ) )
        configs.remove( entry.getKey() );

      configs.remove( "mapred.working.dir" );
      configs.remove( "mapreduce.job.working.dir" ); // hadoop2
      }

    return configs;
    }

  public static JobConf[] getJobConfs( Configuration job, List<Map<String, String>> configs )
    {
    JobConf[] jobConfs = new JobConf[ configs.size() ];

    for( int i = 0; i < jobConfs.length; i++ )
      jobConfs[ i ] = (JobConf) mergeConf( job, configs.get( i ), false );

    return jobConfs;
    }

  public static <J extends Configuration> J mergeConf( J job, Map<String, String> config, boolean directly )
    {
    Configuration currentConf = directly ? job : ( job instanceof JobConf ? copyJobConf( (JobConf) job ) : new Configuration( job ) );

    for( String key : config.keySet() )
      {
      LOG.debug( "merging key: {} value: {}", key, config.get( key ) );

      currentConf.set( key, config.get( key ) );
      }

    return (J) currentConf;
    }

  public static Configuration removePropertiesFrom( Configuration jobConf, String... keys )
    {
    Map<Object, Object> properties = createProperties( jobConf );

    for( String key : keys )
      properties.remove( key );

    return copyConfiguration( properties, new JobConf() );
    }

  public static boolean removeStateFromDistCache( Configuration conf, String path ) throws IOException
    {
    return new Hfs( new TextLine(), path ).deleteResource( conf );
    }

  public static PlatformInfo getPlatformInfo()
    {
    if( platformInfo == null )
      platformInfo = getPlatformInfoInternal( JobConf.class, "org/apache/hadoop", "Hadoop" );

    return platformInfo;
    }

  public static PlatformInfo getPlatformInfo( Class type, String attributePath, String platformName )
    {
    if( platformInfo == null )
      platformInfo = getPlatformInfoInternal( type, attributePath, platformName );

    return platformInfo;
    }

  public static PlatformInfo createPlatformInfo( Class type, String attributePath, String platformName )
    {
    return getPlatformInfoInternal( type, attributePath, platformName );
    }

  private static PlatformInfo getPlatformInfoInternal( Class type, String attributePath, String platformName )
    {
    URL url = type.getResource( type.getSimpleName() + ".class" );

    if( url == null || !url.toString().startsWith( "jar" ) )
      return new PlatformInfo( platformName, null, null );

    String path = url.toString();
    path = path.substring( 0, path.lastIndexOf( "!" ) + 1 );

    String manifestPath = path + "/META-INF/MANIFEST.MF";
    String parsedVersion = Util.findVersion( path.substring( 0, path.length() - 1 ) );

    Manifest manifest;

    try
      {
      manifest = new Manifest( new URL( manifestPath ).openStream() );
      }
    catch( IOException exception )
      {
      LOG.warn( "unable to get manifest from {}: {}", manifestPath, exception.getMessage() );

      return new PlatformInfo( platformName, null, parsedVersion );
      }

    Attributes attributes = manifest.getAttributes( attributePath );

    if( attributes == null )
      attributes = manifest.getMainAttributes();

    if( attributes == null )
      {
      LOG.debug( "unable to get platform manifest attributes" );
      return new PlatformInfo( platformName, null, parsedVersion );
      }

    String vendor = attributes.getValue( "Implementation-Vendor" );
    String version = attributes.getValue( "Implementation-Version" );

    if( Util.isEmpty( version ) )
      version = parsedVersion;

    return new PlatformInfo( platformName, vendor, version );
    }

  /**
   * Copies paths from one local path to a remote path. If syncTimes is true, both modification and access time are
   * changed to match the local 'from' path.
   * <p/>
   * Returns a map of file-name to remote modification times if the remote time is different than the local time.
   *
   * @param config
   * @param commonPaths
   * @param syncTimes
   */
  public static Map<String, Long> syncPaths( Configuration config, Map<Path, Path> commonPaths, boolean syncTimes )
    {
    if( commonPaths == null )
      return Collections.emptyMap();

    Map<String, Long> timestampMap = new HashMap<>();

    Map<Path, Path> copyPaths = getCopyPaths( config, commonPaths ); // tests remote file existence or if stale

    LocalFileSystem localFS = getLocalFS( config );
    FileSystem remoteFS = getDefaultFS( config );

    for( Map.Entry<Path, Path> entry : copyPaths.entrySet() )
      {
      Path localPath = entry.getKey();
      Path remotePath = entry.getValue();

      try
        {
        LOG.info( "copying from: {}, to: {}", localPath, remotePath );
        remoteFS.copyFromLocalFile( localPath, remotePath );

        if( !syncTimes )
          {
          timestampMap.put( remotePath.getName(), remoteFS.getFileStatus( remotePath ).getModificationTime() );
          continue;
          }
        }
      catch( IOException exception )
        {
        throw new FlowException( "unable to copy local: " + localPath + " to remote: " + remotePath, exception );
        }

      FileStatus localFileStatus = null;

      try
        {
        // sync the modified times so we can lazily upload jars to hdfs after job is started
        // otherwise modified time will be local to hdfs
        localFileStatus = localFS.getFileStatus( localPath );
        remoteFS.setTimes( remotePath, localFileStatus.getModificationTime(), -1 ); // don't set the access time
        }
      catch( IOException exception )
        {
        LOG.info( "unable to set local modification time on remote file: {}, 'dfs.namenode.accesstime.precision' may be set to 0 on HDFS.", remotePath );

        if( localFileStatus != null )
          timestampMap.put( remotePath.getName(), localFileStatus.getModificationTime() );
        }
      }

    return timestampMap;
    }

  public static Map<Path, Path> getCommonPaths( Map<String, Path> localPaths, Map<String, Path> remotePaths )
    {
    Map<Path, Path> commonPaths = new HashMap<Path, Path>();

    for( Map.Entry<String, Path> entry : localPaths.entrySet() )
      {
      if( remotePaths.containsKey( entry.getKey() ) )
        commonPaths.put( entry.getValue(), remotePaths.get( entry.getKey() ) );
      }

    return commonPaths;
    }

  private static Map<Path, Path> getCopyPaths( Configuration config, Map<Path, Path> commonPaths )
    {
    Map<Path, Path> copyPaths = new HashMap<Path, Path>();

    FileSystem remoteFS = getDefaultFS( config );
    FileSystem localFS = getLocalFS( config );

    for( Map.Entry<Path, Path> entry : commonPaths.entrySet() )
      {
      Path localPath = entry.getKey();
      Path remotePath = entry.getValue();

      try
        {
        boolean localExists = localFS.exists( localPath );
        boolean remoteExist = remoteFS.exists( remotePath );

        if( localExists && !remoteExist )
          {
          copyPaths.put( localPath, remotePath );
          }
        else if( localExists )
          {
          long localModTime = localFS.getFileStatus( localPath ).getModificationTime();
          long remoteModTime = remoteFS.getFileStatus( remotePath ).getModificationTime();

          if( localModTime > remoteModTime )
            copyPaths.put( localPath, remotePath );
          }
        }
      catch( IOException exception )
        {
        throw new FlowException( "unable to get handle to underlying filesystem", exception );
        }
      }

    return copyPaths;
    }

  public static void resolvePaths( Configuration config, Collection<String> classpath, String remoteRoot, String resourceSubPath, Map<String, Path> localPaths, Map<String, Path> remotePaths )
    {
    FileSystem defaultFS = getDefaultFS( config );
    FileSystem localFS = getLocalFS( config );

    Path remoteRootPath = new Path( remoteRoot == null ? "./.staging" : remoteRoot );

    if( resourceSubPath != null )
      remoteRootPath = new Path( remoteRootPath, resourceSubPath );

    remoteRootPath = defaultFS.makeQualified( remoteRootPath );

    boolean defaultIsLocal = defaultFS.equals( localFS );

    for( String stringPath : classpath )
      {
      Path path = new Path( stringPath );

      URI uri = path.toUri();

      if( uri.getScheme() == null && !defaultIsLocal ) // we want to sync
        {
        Path localPath = localFS.makeQualified( path );

        if( !exists( localFS, localPath ) )
          throw new FlowException( "path not found: " + localPath );

        String name = localPath.getName();

        if( resourceSubPath != null )
          name = resourceSubPath + "/" + name;

        localPaths.put( name, localPath );
        remotePaths.put( name, defaultFS.makeQualified( new Path( remoteRootPath, path.getName() ) ) );
        }
      else if( localFS.equals( getFileSystem( config, path ) ) )
        {
        if( !exists( localFS, path ) )
          throw new FlowException( "path not found: " + path );

        Path localPath = localFS.makeQualified( path );

        String name = localPath.getName();

        if( resourceSubPath != null )
          name = resourceSubPath + "/" + name;

        localPaths.put( name, localPath );
        }
      else
        {
        if( !exists( defaultFS, path ) )
          throw new FlowException( "path not found: " + path );

        Path defaultPath = defaultFS.makeQualified( path );

        String name = defaultPath.getName();

        if( resourceSubPath != null )
          name = resourceSubPath + "/" + name;

        remotePaths.put( name, defaultPath );
        }
      }
    }

  private static boolean exists( FileSystem fileSystem, Path path )
    {
    try
      {
      return fileSystem.exists( path );
      }
    catch( IOException exception )
      {
      throw new FlowException( "could not test file exists: " + path );
      }
    }

  private static FileSystem getFileSystem( Configuration config, Path path )
    {
    try
      {
      return path.getFileSystem( config );
      }
    catch( IOException exception )
      {
      throw new FlowException( "unable to get handle to underlying filesystem", exception );
      }
    }

  public static LocalFileSystem getLocalFS( Configuration config )
    {
    try
      {
      return FileSystem.getLocal( config );
      }
    catch( IOException exception )
      {
      throw new FlowException( "unable to get handle to underlying filesystem", exception );
      }
    }

  public static FileSystem getDefaultFS( Configuration config )
    {
    try
      {
      return FileSystem.get( config );
      }
    catch( IOException exception )
      {
      throw new FlowException( "unable to get handle to underlying filesystem", exception );
      }
    }

  public static boolean isLocal( Configuration conf )
    {
    // hadoop 1.0 and 2.0 use different properties to define local mode: we check the new YARN
    // property first
    String frameworkName = conf.get( "mapreduce.framework.name" );

    // we are running on hadoop 2.0 (YARN)
    if( frameworkName != null )
      return frameworkName.equals( "local" );

    // for Tez
    String tezLocal = conf.get( "tez.local.mode" );

    if( tezLocal != null )
      return tezLocal.equals( "true" );

    // hadoop 1.0: use the old property to determine the local mode
    String hadoop1 = conf.get( "mapred.job.tracker" );

    if( hadoop1 == null )
      {
      LOG.warn( "could not successfully test if Hadoop based platform is in standalone/local mode, no valid properties set, returning false - tests for: mapreduce.framework.name, tez.local.mode, and mapred.job.tracker" );
      return false;
      }

    return hadoop1.equals( "local" );
    }

  public static boolean isYARN( Configuration conf )
    {
    return conf.get( "mapreduce.framework.name" ) != null;
    }

  public static void setLocal( Configuration conf )
    {
    // set both properties to local
    conf.set( "mapred.job.tracker", "local" );

    // yarn
    conf.set( "mapreduce.framework.name", "local" );

    // tez
    conf.set( "tez.local.mode", "true" );
    conf.set( "tez.runtime.optimize.local.fetch", "true" );
    }

  private static boolean interfaceAssignableFromClassName(Class<?> xface, String className)
    {
      if ((className == null) || (xface == null))
        return false;

      try {
        Class<?> klass = Class.forName(className);
        if (klass == null)
          return false;

        if (!xface.isAssignableFrom(klass))
          return false;

        return true;
      } catch (ClassNotFoundException cnfe) {
        return false; // let downstream figure it out
      }
    }


  public static boolean setNewApi( Configuration conf, String className )
    {
    if( className == null ) // silently return and let the error be caught downstream
      return false;

    boolean isStable = className.startsWith( "org.apache.hadoop.mapred." )
            || interfaceAssignableFromClassName(org.apache.hadoop.mapred.InputFormat.class, className);

    boolean isNew = className.startsWith( "org.apache.hadoop.mapreduce." )
            || interfaceAssignableFromClassName(org.apache.hadoop.mapreduce.InputFormat.class, className);

    if( isStable )
      conf.setBoolean( "mapred.mapper.new-api", false );
    else if( isNew )
      conf.setBoolean( "mapred.mapper.new-api", true );
    else
      throw new IllegalStateException( "cannot determine if class denotes stable or new api, please set 'mapred.mapper.new-api' to the appropriate value" );

    return true;
    }

  public static void addInputPath( Configuration conf, Path path )
    {
    Path workingDirectory = getWorkingDirectory( conf );
    path = new Path( workingDirectory, path );
    String dirStr = StringUtils.escapeString( path.toString() );
    String dirs = conf.get( "mapred.input.dir" );
    conf.set( "mapred.input.dir", dirs == null ? dirStr :
      dirs + StringUtils.COMMA_STR + dirStr );
    }

  public static void setOutputPath( Configuration conf, Path path )
    {
    Path workingDirectory = getWorkingDirectory( conf );
    path = new Path( workingDirectory, path );
    conf.set( "mapred.output.dir", path.toString() );
    }

  private static Path getWorkingDirectory( Configuration conf )
    {
    String name = conf.get( "mapred.working.dir" );
    if( name != null )
      {
      return new Path( name );
      }
    else
      {
      try
        {
        Path dir = FileSystem.get( conf ).getWorkingDirectory();
        conf.set( "mapred.working.dir", dir.toString() );
        return dir;
        }
      catch( IOException e )
        {
        throw new RuntimeException( e );
        }
      }
    }

  public static Path getOutputPath( Configuration conf )
    {
    String name = conf.get( "mapred.output.dir" );
    return name == null ? null : new Path( name );
    }

  public static String pack( Object object, Configuration conf )
    {
    if( object == null )
      return "";

    try
      {
      return serializeBase64( object, conf, true );
      }
    catch( IOException exception )
      {
      throw new FlowException( "unable to pack object: " + object.getClass().getCanonicalName(), exception );
      }
    }

  public static void addFields( Configuration conf, String property, Map<Integer, Fields> fields )
    {
    if( fields == null || fields.isEmpty() )
      return;

    Map<String, Fields> toPack = new HashMap<>();

    for( Map.Entry<Integer, Fields> entry : fields.entrySet() )
      toPack.put( entry.getKey().toString(), entry.getValue() );

    conf.set( property, pack( toPack, conf ) );
    }

  public static Map<Integer, Fields> getFields( Configuration conf, String property ) throws IOException
    {
    String value = conf.getRaw( property );

    if( value == null || value.isEmpty() )
      return Collections.emptyMap();

    Map<String, Fields> map = deserializeBase64( value, conf, Map.class, true );
    Map<Integer, Fields> result = new HashMap<>();

    for( Map.Entry<String, Fields> entry : map.entrySet() )
      result.put( Integer.parseInt( entry.getKey() ), entry.getValue() );

    return result;
    }

  public static void addComparators( Configuration conf, String property, Map<String, Fields> map, BaseFlowStep flowStep, Group group )
    {
    Iterator<Fields> fieldsIterator = map.values().iterator();

    if( !fieldsIterator.hasNext() )
      return;

    Fields fields = fieldsIterator.next();

    if( fields.hasComparators() )
      {
      conf.set( property, pack( fields, conf ) );
      return;
      }

    // use resolved fields if there are no comparators.
    Set<Scope> previousScopes = flowStep.getPreviousScopes( group );

    fields = previousScopes.iterator().next().getOutValuesFields();

    if( fields.size() != 0 ) // allows fields.UNKNOWN to be used
      conf.setInt( property + ".size", fields.size() );
    }

  public static void addComparators( Configuration conf, String property, Map<String, Fields> map, Fields resolvedFields )
    {
    Iterator<Fields> fieldsIterator = map.values().iterator();

    if( !fieldsIterator.hasNext() )
      return;

    while( fieldsIterator.hasNext() )
      {
      Fields fields = fieldsIterator.next();

      if( fields.hasComparators() )
        {
        conf.set( property, pack( fields, conf ) );
        return;
        }
      }

    if( resolvedFields.size() != 0 ) // allows fields.UNKNOWN to be used
      conf.setInt( property + ".size", resolvedFields.size() );
    }
  }