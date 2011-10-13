/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

package cascading.tap.hadoop;

import java.beans.ConstructorProperties;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.hadoop.HadoopUtil;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleEntrySchemeIterator;
import cascading.tuple.hadoop.TupleSerialization;
import cascading.util.Util;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3native.NativeS3FileSystem;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class Hfs is the base class for all Hadoop file system access. Use {@link cascading.tap.hadoop.Dfs}, or {@link Lfs}
 * for resources specific to Hadoop Distributed file system, the Local file system, or Amazon S3, respectively.
 * <p/>
 * Use the Hfs class if the 'kind' of resource is unknown at design time. To use, prefix a scheme to the 'stringPath'. Where
 * <code>hdfs://...</code> will denote Dfs, <code>file://...</code> will denote Lfs, and
 * <code>s3://aws_id:aws_secret@bucket/...</code> will denote S3fs.
 * <p/>
 * Call {@link #setTemporaryDirectory(java.util.Map, String)} to use a different temporary file directory path
 * other than the current Hadoop default path.
 */
public class Hfs extends Tap<HadoopFlowProcess, JobConf, RecordReader, OutputCollector>
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( Hfs.class );

  /** Field TEMPORARY_DIRECTORY */
  private static final String TEMPORARY_DIRECTORY = "cascading.tmp.dir";

  /** Field stringPath */
  String stringPath;
  /** Field uriScheme */
  transient URI uriScheme;
  /** Field path */
  transient Path path;
  /** Field paths */
  private transient FileStatus[] statuses;

  /**
   * Method setTemporaryDirectory sets the temporary directory on the given properties object.
   *
   * @param properties of type Map<Object,Object>
   * @param tempDir    of type String
   */
  public static void setTemporaryDirectory( Map<Object, Object> properties, String tempDir )
    {
    properties.put( TEMPORARY_DIRECTORY, tempDir );
    }

  /**
   * Method getTemporaryDirectory returns the configured temporary directory from the given properties object.
   *
   * @param properties of type Map<Object,Object>
   * @return a String or null if not set
   */
  public static String getTemporaryDirectory( Map<Object, Object> properties )
    {
    return (String) properties.get( TEMPORARY_DIRECTORY );
    }

  protected Hfs()
    {
    }

  @ConstructorProperties({"scheme"})
  protected Hfs( Scheme scheme )
    {
    super( scheme );
    }

  /**
   * Constructor Hfs creates a new Hfs instance.
   *
   * @param fields     of type Fields
   * @param stringPath of type String
   */
  @Deprecated
  @ConstructorProperties({"fields", "stringPath"})
  public Hfs( Fields fields, String stringPath )
    {
    super( new SequenceFile( fields ) );
    setStringPath( stringPath );
    }

  /**
   * Constructor Hfs creates a new Hfs instance.
   *
   * @param fields     of type Fields
   * @param stringPath of type String
   * @param replace    of type boolean
   */
  @Deprecated
  @ConstructorProperties({"fields", "stringPath", "replace"})
  public Hfs( Fields fields, String stringPath, boolean replace )
    {
    super( new SequenceFile( fields ), replace ? SinkMode.REPLACE : SinkMode.KEEP );
    setStringPath( stringPath );
    }

  /**
   * Constructor Hfs creates a new Hfs instance.
   *
   * @param fields     of type Fields
   * @param stringPath of type String
   * @param sinkMode   of type SinkMode
   */
  @Deprecated
  @ConstructorProperties({"fields", "stringPath", "sinkMode"})
  public Hfs( Fields fields, String stringPath, SinkMode sinkMode )
    {
    super( new SequenceFile( fields ), sinkMode );
    setStringPath( stringPath );

    if( sinkMode == SinkMode.UPDATE )
      throw new IllegalArgumentException( "updates are not supported" );
    }

  /**
   * Constructor Hfs creates a new Hfs instance.
   *
   * @param scheme     of type Scheme
   * @param stringPath of type String
   */
  @ConstructorProperties({"scheme", "stringPath"})
  public Hfs( Scheme scheme, String stringPath )
    {
    super( scheme );
    setStringPath( stringPath );
    }

  /**
   * Constructor Hfs creates a new Hfs instance.
   *
   * @param scheme     of type Scheme
   * @param stringPath of type String
   * @param replace    of type boolean
   */
  @Deprecated
  @ConstructorProperties({"scheme", "stringPath", "replace"})
  public Hfs( Scheme scheme, String stringPath, boolean replace )
    {
    super( scheme, replace ? SinkMode.REPLACE : SinkMode.KEEP );
    setStringPath( stringPath );
    }

  /**
   * Constructor Hfs creates a new Hfs instance.
   *
   * @param scheme     of type Scheme
   * @param stringPath of type String
   * @param sinkMode   of type SinkMode
   */
  @ConstructorProperties({"scheme", "stringPath", "sinkMode"})
  public Hfs( Scheme scheme, String stringPath, SinkMode sinkMode )
    {
    super( scheme, sinkMode );
    setStringPath( stringPath );
    }

  protected void setStringPath( String stringPath )
    {
    this.stringPath = Util.normalizeUrl( stringPath );
    }

  protected void setUriScheme( URI uriScheme )
    {
    this.uriScheme = uriScheme;
    }

  public URI getURIScheme( JobConf jobConf )
    {
    if( uriScheme != null )
      return uriScheme;

    uriScheme = makeURIScheme( jobConf );

    return uriScheme;
    }

  protected URI makeURIScheme( JobConf jobConf )
    {
    try
      {
      URI uriScheme = null;

      LOG.debug( "handling path: {}", stringPath );

      URI uri = new URI( stringPath );
      String schemeString = uri.getScheme();
      String authority = uri.getAuthority();

      if( LOG.isDebugEnabled() )
        {
        LOG.debug( "found scheme: {}", schemeString );
        LOG.debug( "found authority: {}", authority );
        }

      if( schemeString != null && authority != null )
        uriScheme = new URI( schemeString + "://" + uri.getAuthority() );
      else if( schemeString != null )
        uriScheme = new URI( schemeString + ":///" );
      else
        uriScheme = getDefaultFileSystemURIScheme( jobConf );

      LOG.debug( "using uri scheme: {}", uriScheme );

      return uriScheme;
      }
    catch( URISyntaxException exception )
      {
      throw new TapException( "could not determine scheme from path: " + getIdentifier(), exception );
      }
    }

  /**
   * Method getDefaultFileSystemURIScheme returns the URI scheme for the default Hadoop FileSystem.
   *
   * @param jobConf of type JobConf
   * @return URI
   */
  public URI getDefaultFileSystemURIScheme( JobConf jobConf )
    {
    return getDefaultFileSystem( jobConf ).getUri();
    }

  protected FileSystem getDefaultFileSystem( JobConf jobConf )
    {
    try
      {
      return FileSystem.get( jobConf );
      }
    catch( IOException exception )
      {
      throw new TapException( "unable to get handle to underlying filesystem", exception );
      }
    }

  protected FileSystem getFileSystem( JobConf jobConf )
    {
    URI scheme = getURIScheme( jobConf );

    try
      {
      return FileSystem.get( scheme, jobConf );
      }
    catch( IOException exception )
      {
      throw new TapException( "unable to get handle to get filesystem for: " + scheme.getScheme(), exception );
      }
    }

  /** @see Tap#getIdentifier() */
  @Override
  public String getIdentifier()
    {
    return getPath().toString();
    }

  public Path getPath()
    {
    if( path != null )
      return path;

    if( stringPath == null )
      throw new IllegalStateException( "path not initialized" );

    path = new Path( stringPath );

    return path;
    }

  @Override
  public String getFullIdentifier( JobConf conf )
    {
    return getPath().makeQualified( getFileSystem( conf ) ).toString();
    }

  @Override
  public void sourceConfInit( HadoopFlowProcess process, JobConf conf )
    {
    Path qualifiedPath = new Path( getFullIdentifier( conf ) );

    for( Path exitingPath : FileInputFormat.getInputPaths( conf ) )
      {
      if( exitingPath.equals( qualifiedPath ) )
        throw new TapException( "may not add duplicate paths, found: " + exitingPath );
      }

    FileInputFormat.addInputPath( conf, qualifiedPath );

    super.sourceConfInit( null, conf );

    makeLocal( conf, qualifiedPath, "forcing job to local mode, via source: " );

    TupleSerialization.setSerializations( conf ); // allows Hfs to be used independent of Flow
    }

  @Override
  public void sinkConfInit( HadoopFlowProcess process, JobConf conf )
    {
    Path qualifiedPath = new Path( getFullIdentifier( conf ) );

    FileOutputFormat.setOutputPath( conf, qualifiedPath );
    super.sinkConfInit( null, conf );

    makeLocal( conf, qualifiedPath, "forcing job to local mode, via sink: " );

    TupleSerialization.setSerializations( conf ); // allows Hfs to be used independent of Flow
    }

  private void makeLocal( JobConf conf, Path qualifiedPath, String infoMessage )
    {
    if( !conf.get( "mapred.job.tracker", "" ).equalsIgnoreCase( "local" ) && qualifiedPath.toUri().getScheme().equalsIgnoreCase( "file" ) )
      {
      if( LOG.isInfoEnabled() )
        LOG.info( infoMessage + toString() );

      conf.set( "mapred.job.tracker", "local" ); // force job to run locally
      }
    }

  public TupleEntryIterator openForRead( HadoopFlowProcess flowProcess, RecordReader input ) throws IOException
    {
    if( input != null )
      return new TupleEntrySchemeIterator( flowProcess, getScheme(), new RecordReaderIterator( input ) );

    Map<Object, Object> properties = HadoopUtil.createProperties( flowProcess.getJobConf() );

    properties.remove( "mapred.input.dir" );

    JobConf conf = HadoopUtil.createJobConf( properties, null );

    return new TupleEntrySchemeIterator( flowProcess, getScheme(), new MultiRecordReaderIterator( this, conf ) );
    }

  public TupleEntryCollector openForWrite( HadoopFlowProcess flowProcess, OutputCollector output ) throws IOException
    {
    if( output != null )
      return super.openForWrite( flowProcess, output );

    HadoopTapCollector schemeCollector = new HadoopTapCollector( flowProcess, this );

    schemeCollector.prepare();

    return schemeCollector;
    }

  @Override
  public boolean createResource( JobConf conf ) throws IOException
    {
    if( LOG.isDebugEnabled() )
      LOG.debug( "making dirs: " + getFullIdentifier( conf ) );

    return getFileSystem( conf ).mkdirs( new Path( getIdentifier() ) );
    }

  @Override
  public boolean deleteResource( JobConf conf ) throws IOException
    {
    if( LOG.isDebugEnabled() )
      LOG.debug( "deleting: {}", getFullIdentifier( conf ) );

    // do not delete the root directory
    if( new Path( getFullIdentifier( conf ) ).depth() == 0 )
      return true;

    FileSystem fileSystem = getFileSystem( conf );

    try
      {
      return fileSystem.delete( new Path( getIdentifier() ), true );
      }
    catch( NullPointerException exception )
      {
      // hack to get around npe thrown when fs reaches root directory
      if( !( fileSystem instanceof NativeS3FileSystem ) )
        throw exception;
      }

    return true;
    }

  @Override
  public boolean resourceExists( JobConf conf ) throws IOException
    {
    return getFileSystem( conf ).exists( new Path( getIdentifier() ) );
    }

  public boolean isDirectory( JobConf conf ) throws IOException
    {
    if( !resourceExists( conf ) )
      return false;

    return getFileSystem( conf ).getFileStatus( new Path( getIdentifier() ) ).isDir();
    }

  public long getSize( JobConf conf ) throws IOException
    {
    if( !resourceExists( conf ) )
      return 0;

    FileStatus fileStatus = getFileSystem( conf ).getFileStatus( new Path( getIdentifier() ) );

    if( fileStatus.isDir() )
      return 0;

    return getFileSystem( conf ).getFileStatus( new Path( getIdentifier() ) ).getLen();
    }

  public long getBlockSize( JobConf conf ) throws IOException
    {
    if( !resourceExists( conf ) )
      return 0;

    FileStatus fileStatus = getFileSystem( conf ).getFileStatus( new Path( getIdentifier() ) );

    if( fileStatus.isDir() )
      return 0;

    return fileStatus.getBlockSize();
    }

  public int getReplication( JobConf conf ) throws IOException
    {
    if( !resourceExists( conf ) )
      return 0;

    FileStatus fileStatus = getFileSystem( conf ).getFileStatus( new Path( getIdentifier() ) );

    if( fileStatus.isDir() )
      return 0;

    return fileStatus.getReplication();
    }

  public String[] getChildIdentifiers( JobConf conf ) throws IOException
    {
    if( !resourceExists( conf ) )
      return new String[ 0 ];

    FileStatus[] statuses = getFileSystem( conf ).listStatus( new Path( getIdentifier() ) );

    String[] children = new String[ statuses.length ];

    for( int i = 0; i < statuses.length; i++ )
      children[ i ] = statuses[ i ].getPath().getName();

    return children;
    }

  @Override
  public long getModifiedTime( JobConf conf ) throws IOException
    {
    if( !resourceExists( conf ) )
      return 0;

    FileStatus fileStatus = getFileSystem( conf ).getFileStatus( new Path( getIdentifier() ) );

    if( !fileStatus.isDir() )
      return fileStatus.getModificationTime();

    makeStatuses( conf );

    // statuses is empty, return 0
    if( statuses == null || statuses.length == 0 )
      return 0;

    long date = 0;

    // filter out directories as we don't recurs into sub dirs
    for( FileStatus status : statuses )
      {
      if( !status.isDir() )
        date = Math.max( date, status.getModificationTime() );
      }

    return date;
    }

  protected Path getTempPath( JobConf conf )
    {
    String tempDir = conf.get( TEMPORARY_DIRECTORY );

    if( tempDir == null )
      tempDir = conf.get( "hadoop.tmp.dir" );

    return new Path( tempDir );
    }

  protected String makeTemporaryPathDirString( String name )
    {
    // _ is treated as a hidden file, so wipe them out
    name = name.replaceAll( "^[_\\W\\s]+", "" );

    if( name.isEmpty() )
      name = "temp-path";

    return name.replaceAll( "[\\W\\s]+", "_" ) + Integer.toString( (int) ( 10000000 * Math.random() ) );
    }

  /**
   * Given a file-system object, it makes an array of paths
   *
   * @param conf of type JobConf
   * @throws IOException on failure
   */
  private void makeStatuses( JobConf conf ) throws IOException
    {
    if( statuses != null )
      return;

    statuses = getFileSystem( conf ).listStatus( new Path( getIdentifier() ) );
    }

  /** @see Object#toString() */
  @Override
  public String toString()
    {
    if( stringPath != null )
      return getClass().getSimpleName() + "[\"" + getScheme() + "\"]" + "[\"" + Util.sanitizeUrl( stringPath ) + "\"]"; // sanitize
    else
      return getClass().getSimpleName() + "[\"" + getScheme() + "\"]" + "[not initialized]";
    }

  /** @see Tap#equals(Object) */
  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( object == null || getClass() != object.getClass() )
      return false;
    if( !super.equals( object ) )
      return false;

    Hfs hfs = (Hfs) object;

    if( stringPath != null ? !stringPath.equals( hfs.stringPath ) : hfs.stringPath != null )
      return false;

    return true;
    }

  /** @see Tap#hashCode() */
  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( stringPath != null ? stringPath.hashCode() : 0 );
    return result;
    }
  }
