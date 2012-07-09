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

package cascading.flow.hadoop.util;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import cascading.flow.FlowException;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class HadoopUtil
  {
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger( HadoopUtil.class );
  public static final String ENCODING = "US-ASCII";
  public static final String SERIALIZATION_FACTORY_KEY = "cascading.flow.serializer";
  public static final Class<?> DEFAULT_FLOW_SERIALIZER = JavaFlowSerializer.class;

  public static void initLog4j( JobConf jobConf )
    {
    String values = jobConf.get( "log4j.logger", null );

    if( values == null || values.length() == 0 )
      return;

    String[] elements = values.split( "," );

    for( String element : elements )
      {
      String[] logger = element.split( "=" );

      Logger.getLogger( logger[ 0 ] ).setLevel( Level.toLevel( logger[ 1 ] ) );
      }
    }

  public static JobConf createJobConf( Map<Object, Object> properties, JobConf defaultJobconf )
    {
    JobConf jobConf = defaultJobconf == null ? new JobConf() : new JobConf( defaultJobconf );

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
      if( value instanceof Class || value instanceof JobConf )
        continue;

      jobConf.set( key.toString(), value.toString() );
      }

    return jobConf;
    }

  public static Map<Object, Object> createProperties( Configuration jobConf )
    {
    Map<Object, Object> properties = new HashMap<Object, Object>();

    for( Map.Entry<String, String> entry : jobConf )
      properties.put( entry.getKey(), entry.getValue() );

    return properties;
    }

  public static Thread getHDFSShutdownHook()
    {
    Exception caughtException = null;

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

  public static String encodeBytes(byte[] bytes)
    {
    try
      {
      return new String(Base64.encodeBase64(bytes), ENCODING);
      }
    catch (UnsupportedEncodingException e)
      {
      throw new RuntimeException(e);
      }
    }

  public static byte[] decodeBytes(String str)
    {
    try
      {
      byte[] bytes = str.getBytes(ENCODING);
      return Base64.decodeBase64(bytes);
      }
    catch (UnsupportedEncodingException e)
      {
      throw new RuntimeException(e);
      }
    }

  public static <T> FlowSerializer instantiateSerializer(Configuration conf, Class<T> klass)
    throws ClassNotFoundException
    {
    Class<FlowSerializer> flowSerializerClass;

    String serializerClassName = conf.get(SERIALIZATION_FACTORY_KEY);

    if (serializerClassName == null || serializerClassName.length() == 0)
      {
      flowSerializerClass = (Class<FlowSerializer>) DEFAULT_FLOW_SERIALIZER;
      }
    else
      {
      flowSerializerClass = (Class<FlowSerializer>) Class.forName(serializerClassName);
      }

    FlowSerializer flowSerializer;

    try
      {
      flowSerializer = flowSerializerClass.newInstance();

      if (Configured.class.isAssignableFrom(flowSerializerClass))
        ((Configured) flowSerializer).setConf(conf);
      }

    catch (Exception e)
      {
      e.printStackTrace();
      throw new IllegalArgumentException("Unable to instantiate serializer \""
        + flowSerializerClass.getName()
        + "\" for class: "
        + klass.getName());
      }

    if (!flowSerializer.accepts(klass))
      throw new IllegalArgumentException(serializerClassName + " won't accept objects of class " + klass.toString());

    return flowSerializer;
    }

  public static <T> String serializeBase64( T object, JobConf conf ) throws IOException
    {
    return serializeBase64( object, conf, true );
    }

  public static <T> String serializeBase64( T object, JobConf conf, boolean compress ) throws IOException
    {
    FlowSerializer flowSerializer;
    try
      {
      flowSerializer = instantiateSerializer(conf, object.getClass());
      }
    catch (ClassNotFoundException e)
      {
      throw new IOException(e);
      }

    return encodeBytes(flowSerializer.serialize(object, compress));
    }

  /**
   * This method deserializes the Base64 encoded String into an Object instance.
   *
   * @param string
   * @return an Object
   */
  public static <T> T deserializeBase64( String string, Configuration conf, Class<T> klass) throws IOException
    {
    return deserializeBase64( string, conf, klass, true );
    }

  public static  <T> T deserializeBase64( String string, Configuration conf, Class<T> klass, boolean decompress ) throws IOException
    {
    if( string == null || string.length() == 0 )
      return null;

    FlowSerializer flowSerializer;

    try
      {
      flowSerializer = instantiateSerializer(conf, klass);
      }
    catch (ClassNotFoundException e)
      {
      throw new IOException(e);
      }

    return flowSerializer.deserialize(decodeBytes(string), klass, decompress);
    }

  public static Class findMainClass( Class defaultType )
    {
    StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();

    for( StackTraceElement stackTraceElement : stackTrace )
      {
      if( stackTraceElement.getMethodName().equals( "main" ) && !stackTraceElement.getClassName().startsWith( "org.apache.hadoop" ) )
        {
        try
          {
          LOG.info( "resolving application jar from found main method on: {}", stackTraceElement.getClassName() );

          return Thread.currentThread().getContextClassLoader().loadClass( stackTraceElement.getClassName() );
          }
        catch( ClassNotFoundException exception )
          {
          LOG.warn( "unable to load class while discovering application jar: {}", stackTraceElement.getClassName(), exception );
          }
        }
      }

    LOG.info( "using default application jar, may cause class not found exceptions on the cluster" );

    return defaultType;
    }

  public static Map<String, String> getConfig( JobConf defaultConf, JobConf updatedConf )
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
      }

    return configs;
    }

  public static JobConf[] getJobConfs( JobConf job, List<Map<String, String>> configs )
    {
    JobConf[] jobConfs = new JobConf[ configs.size() ];

    for( int i = 0; i < jobConfs.length; i++ )
      jobConfs[ i ] = mergeConf( job, configs.get( i ), false );

    return jobConfs;
    }

  public static JobConf mergeConf( JobConf job, Map<String, String> config, boolean directly )
    {
    JobConf currentConf = directly ? job : new JobConf( job );

    for( String key : config.keySet() )
      {
      LOG.debug( "merging key: {} value: {}", key, config.get( key ) );

      currentConf.set( key, config.get( key ) );
      }

    return currentConf;
    }

  public static JobConf removePropertiesFrom( JobConf jobConf, String... keys )
    {
    Map<Object, Object> properties = createProperties( jobConf );

    for( String key : keys )
      properties.remove( key );

    return createJobConf( properties, null );
    }

  public static boolean removeStateFromDistCache( JobConf conf, String path ) throws IOException
    {
    return new Hfs( new TextLine(), path ).deleteResource( conf );
    }

  public static String writeStateToDistCache( JobConf conf, String id, String stepState )
    {
    LOG.info( "writing step state to dist cache, too large for job conf, size: {}", stepState.length() );

    String statePath = Hfs.getTempPath( conf ) + "/step-state-" + id;

    Hfs temp = new Hfs( new TextLine(), statePath, SinkMode.REPLACE );

    try
      {
      TupleEntryCollector writer = temp.openForWrite( new HadoopFlowProcess( conf ) );

      writer.add( new Tuple( stepState ) );

      writer.close();
      }
    catch( IOException exception )
      {
      throw new FlowException( "unable to write step state to Hadoop FS: " + temp.getIdentifier() );
      }

    URI uri = new Path( statePath ).toUri();
    DistributedCache.addCacheFile( uri, conf );

    LOG.info( "using step state path: {}", uri );

    return statePath;
    }

  public static String readStateFromDistCache( JobConf jobConf, String id ) throws IOException
    {
    Path[] files = DistributedCache.getLocalCacheFiles( jobConf );

    Path stepStatePath = null;

    for( Path file : files )
      {
      if( !file.toString().contains( "step-state-" + id ) )
        continue;

      stepStatePath = file;
      break;
      }

    if( stepStatePath == null )
      throw new FlowException( "unable to find step state from distributed cache" );

    LOG.info( "reading step state from local path: {}", stepStatePath );

    Hfs temp = new Lfs( new TextLine( new Fields( "line" ) ), stepStatePath.toString() );

    TupleEntryIterator reader = null;

    try
      {
      reader = temp.openForRead( new HadoopFlowProcess( jobConf ) );

      if( !reader.hasNext() )
        throw new FlowException( "step state path is empty: " + temp.getIdentifier() );

      return reader.next().getString( 0 );
      }
    catch( IOException exception )
      {
      throw new FlowException( "unable to find state path: " + temp.getIdentifier(), exception );
      }
    finally
      {
      reader.close();
      }
    }
  }