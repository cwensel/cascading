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

package cascading.platform.hadoop;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.flow.FlowSession;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.platform.TestPlatform;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.hadoop.TextLine;
import cascading.scheme.util.DelimitedParser;
import cascading.scheme.util.FieldTypeResolver;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.PartitionTap;
import cascading.tap.hadoop.TemplateTap;
import cascading.tap.hadoop.util.Hadoop18TapUtil;
import cascading.tap.partition.Partition;
import cascading.tuple.Fields;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public abstract class BaseHadoopPlatform extends TestPlatform
  {
  private static final Logger LOG = LoggerFactory.getLogger( BaseHadoopPlatform.class );

  public transient static FileSystem fileSys;
  public transient static Configuration jobConf;
  public transient static Map<Object, Object> properties = new HashMap<Object, Object>();

  public int numMapTasks = 4;
  public int numReduceTasks = 1;
  protected String logger;

  public BaseHadoopPlatform()
    {
    logger = System.getProperty( "log4j.logger" );
    }

  @Override
  public boolean isMapReduce()
    {
    return true;
    }

  public void setNumMapTasks( int numMapTasks )
    {
    if( numMapTasks > 0 )
      this.numMapTasks = numMapTasks;
    }

  public void setNumReduceTasks( int numReduceTasks )
    {
    if( numReduceTasks > 0 )
      this.numReduceTasks = numReduceTasks;
    }

  @Override
  public Map<Object, Object> getProperties()
    {
    return new HashMap<Object, Object>( properties );
    }

  @Override
  public void tearDown()
    {
    }

  public JobConf getJobConf()
    {
    return new JobConf( jobConf );
    }

  public boolean isHDFSAvailable()
    {
    try
      {
      FileSystem fileSystem = FileSystem.get( new URI( "hdfs:", null, null ), getJobConf() );

      return fileSystem != null;
      }
    catch( IOException exception ) // if no hdfs, a no filesystem for scheme io exception will be caught
      {
      LOG.warn( "unable to get hdfs filesystem", exception );
      }
    catch( URISyntaxException exception )
      {
      throw new RuntimeException( "internal failure", exception );
      }

    return false;
    }

  @Override
  public FlowProcess getFlowProcess()
    {
    return new HadoopFlowProcess( FlowSession.NULL, getJobConf(), true );
    }

  @Override
  public void copyFromLocal( String inputFile ) throws IOException
    {
    if( !new File( inputFile ).exists() )
      throw new FileNotFoundException( "data file not found: " + inputFile );

    if( !isUseCluster() )
      return;

    Path path = new Path( safeFileName( inputFile ) );

    if( !fileSys.exists( path ) )
      FileUtil.copy( new File( inputFile ), fileSys, path, false, jobConf );
    }

  @Override
  public void copyToLocal( String outputFile ) throws IOException
    {
    if( !isUseCluster() )
      return;

    Path path = new Path( safeFileName( outputFile ) );

    if( !fileSys.exists( path ) )
      throw new FileNotFoundException( "data file not found: " + outputFile );

    File file = new File( outputFile );

    if( file.exists() )
      file.delete();

    if( fileSys.isFile( path ) )
      {
      // its a file, so just copy it over
      FileUtil.copy( fileSys, path, file, false, jobConf );
      return;
      }

    // it's a directory
    file.mkdirs();

    FileStatus contents[] = fileSys.listStatus( path );

    for( FileStatus fileStatus : contents )
      {
      Path currentPath = fileStatus.getPath();

      if( currentPath.getName().startsWith( "_" ) ) // filter out temp and log dirs
        continue;

      FileUtil.copy( fileSys, currentPath, new File( file, currentPath.getName() ), false, jobConf );
      }
    }

  @Override
  public boolean remoteExists( String outputFile ) throws IOException
    {
    return fileSys.exists( new Path( safeFileName( outputFile ) ) );
    }

  @Override
  public boolean remoteRemove( String outputFile, boolean recursive ) throws IOException
    {
    return fileSys.delete( new Path( safeFileName( outputFile ) ), recursive );
    }

  @Override
  public Tap getTap( Scheme scheme, String filename, SinkMode mode )
    {
    return new Hfs( scheme, safeFileName( filename ), mode );
    }

  @Override
  public Tap getTextFile( Fields sourceFields, Fields sinkFields, String filename, SinkMode mode )
    {
    if( sourceFields == null )
      return new Hfs( new TextLine(), safeFileName( filename ), mode );

    return new Hfs( new TextLine( sourceFields, sinkFields ), safeFileName( filename ), mode );
    }

  @Override
  public Tap getDelimitedFile( Fields fields, boolean hasHeader, String delimiter, String quote, Class[] types, String filename, SinkMode mode )
    {
    return new Hfs( new TextDelimited( fields, hasHeader, delimiter, quote, types ), safeFileName( filename ), mode );
    }

  @Override
  public Tap getDelimitedFile( Fields fields, boolean skipHeader, boolean writeHeader, String delimiter, String quote, Class[] types, String filename, SinkMode mode )
    {
    return new Hfs( new TextDelimited( fields, skipHeader, writeHeader, delimiter, quote, types ), safeFileName( filename ), mode );
    }

  @Override
  public Tap getDelimitedFile( String delimiter, String quote, FieldTypeResolver fieldTypeResolver, String filename, SinkMode mode )
    {
    return new Hfs( new TextDelimited( true, new DelimitedParser( delimiter, quote, fieldTypeResolver ) ), safeFileName( filename ), mode );
    }

  @Override
  public Tap getTemplateTap( Tap sink, String pathTemplate, int openThreshold )
    {
    return new TemplateTap( (Hfs) sink, pathTemplate, openThreshold );
    }

  @Override
  public Tap getTemplateTap( Tap sink, String pathTemplate, Fields fields, int openThreshold )
    {
    return new TemplateTap( (Hfs) sink, pathTemplate, fields, openThreshold );
    }

  @Override
  public Tap getPartitionTap( Tap sink, Partition partition, int openThreshold )
    {
    return new PartitionTap( (Hfs) sink, partition, openThreshold );
    }

  @Override
  public Scheme getTestConfigDefScheme()
    {
    return new HadoopConfigDefScheme( new Fields( "line" ) );
    }

  @Override
  public Scheme getTestFailScheme()
    {
    return new HadoopFailScheme( new Fields( "line" ) );
    }

  @Override
  public Comparator getLongComparator( boolean reverseSort )
    {
    return new TestLongComparator( reverseSort );
    }

  @Override
  public Comparator getStringComparator( boolean reverseSort )
    {
    return new TestStringComparator( reverseSort );
    }

  @Override
  public String getHiddenTemporaryPath()
    {
    return Hadoop18TapUtil.TEMPORARY_PATH;
    }

  /**
   * Replaces characters, that are not allowed by HDFS with an "_".
   *
   * @param filename The filename to make safe
   * @return The filename with all non-supported characters removed.
   */
  protected String safeFileName( String filename )
    {
    return filename.replace( ":", "_" ); // not using Util.cleansePathName as it removes /
    }
  }
