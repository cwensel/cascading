/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
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

package cascading.tap;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import cascading.flow.FlowContext;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SequenceFile;
import cascading.tap.hadoop.HadoopOutputCollector;
import cascading.tap.hadoop.HfsCollector;
import cascading.tap.hadoop.HfsIterator;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.hadoop.TupleSerialization;
import cascading.util.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3native.NativeS3FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

/**
 * Class Hfs is the base class for all Hadoop file system access. Use {@link Dfs}, or {@link Lfs}
 * for resources specific to Hadoop Distributed file system, the Local file system, or Amazon S3, respectively.
 * <p/>
 * Use the Hfs class if the 'kind' of resource is unknown at design time. To use, prefix a scheme to the 'stringPath'. Where
 * <code>hdfs://...</code> will denonte Dfs, <code>file://...</code> will denote Lfs, and
 * <code>s3://aws_id:aws_secret@bucket/...</code> will denote S3fs.
 * <p/>
 * Call {@link #setTemporaryDirectory(java.util.Map, String)} to use a different temporary file directory path
 * other than the current Hadoop default path.
 */
public class Hfs extends Tap<Configuration>
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( Hfs.class );

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
   * Methdo getTemporaryDirectory returns the configured temporary directory from the given properties object.
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

  public URI getURIScheme( Configuration conf ) throws IOException
    {
    if( uriScheme != null )
      return uriScheme;

    uriScheme = makeURIScheme( conf );

    return uriScheme;
    }

  protected URI makeURIScheme( Configuration conf ) throws IOException
    {
    try
      {
      URI uriScheme = null;

      if( LOG.isDebugEnabled() )
        LOG.debug( "handling path: " + stringPath );

      URI uri = new URI( stringPath );
      String schemeString = uri.getScheme();
      String authority = uri.getAuthority();

      if( LOG.isDebugEnabled() )
        {
        LOG.debug( "found scheme: " + schemeString );
        LOG.debug( "found authority: " + authority );
        }

      if( schemeString != null && authority != null )
        uriScheme = new URI( schemeString + "://" + uri.getAuthority() );
      else if( schemeString != null )
        uriScheme = new URI( schemeString + ":///" );
      else
        uriScheme = getDefaultFileSystemURIScheme( conf );

      if( LOG.isDebugEnabled() )
        LOG.debug( "using uri scheme: " + uriScheme );

      return uriScheme;
      }
    catch( URISyntaxException exception )
      {
      throw new TapException( "could not determine scheme from path: " + getPath(), exception );
      }
    }

  /**
   * Method getDefaultFileSystemURIScheme returns the URI scheme for the default Hadoop FileSystem.
   *
   * @param conf
   * @return URI
   * @throws IOException when
   */
  public URI getDefaultFileSystemURIScheme( Configuration conf ) throws IOException
    {
    return getDefaultFileSystem( conf ).getUri();
    }

  @Override
  public boolean isWriteDirect()
    {
    return super.isWriteDirect() || stringPath != null && stringPath.matches( "(^https?://.*$)|(^s3tp://.*$)" );
    }

  protected FileSystem getDefaultFileSystem( Configuration conf ) throws IOException
    {
    return FileSystem.get( conf );
    }

  protected FileSystem getFileSystem( Configuration conf ) throws IOException
    {
    return FileSystem.get( getURIScheme( conf ), conf );
    }

  /** @see Tap#getPath() */
  @Override
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
  public Path getQualifiedPath( Job job ) throws IOException
    {
    return getPath().makeQualified( getFileSystem( job.getConfiguration() ) );
    }

  @Override
  public void sourceInit( Job job ) throws IOException
    {
    Path qualifiedPath = getQualifiedPath( job );

    for( Path exitingPath : FileInputFormat.getInputPaths( job ) )
      {
      if( exitingPath.equals( qualifiedPath ) )
        throw new TapException( "may not add duplicate paths, found: " + exitingPath );
      }

    FileInputFormat.addInputPath( job, qualifiedPath );

    super.sourceInit( job );

    makeLocal( job.getConfiguration(), qualifiedPath, "forcing job to local mode, via source: " );

    TupleSerialization.setSerializations( job.getConfiguration() ); // allows Hfs to be used independent of Flow
    }

  @Override
  public void sinkInit( Job job ) throws IOException
    {
    // do not delete if initialized from within a task
    if( isReplace() && job.getConfiguration().get( "mapred.task.partition" ) == null )
      deletePath( job );

    Path qualifiedPath = getQualifiedPath( job );

    FileOutputFormat.setOutputPath( job, qualifiedPath );
    super.sinkInit( job );

    makeLocal( job.getConfiguration(), qualifiedPath, "forcing job to local mode, via sink: " );

    TupleSerialization.setSerializations( job.getConfiguration() ); // allows Hfs to be used independent of Flow
    }

  private void makeLocal( Configuration conf, Path qualifiedPath, String infoMessage )
    {
    if( !conf.get( "mapred.job.tracker", "" ).equalsIgnoreCase( "local" ) && qualifiedPath.toUri().getScheme().equalsIgnoreCase( "file" ) )
      {
      if( LOG.isInfoEnabled() )
        LOG.info( infoMessage + toString() );

      conf.set( "mapred.job.tracker", "local" ); // force job to run locally
      }
    }

  @Override
  public boolean makeDirs( Job job ) throws IOException
    {
    if( LOG.isDebugEnabled() )
      LOG.debug( "making dirs: " + getQualifiedPath( job ) );

    return getFileSystem( job.getConfiguration() ).mkdirs( getPath() );
    }

  @Override
  public boolean deletePath( Job job ) throws IOException
    {
    if( LOG.isDebugEnabled() )
      LOG.debug( "deleting: " + getQualifiedPath( job ) );

    // do not delete the root directory
    if( getQualifiedPath( job ).depth() == 0 )
      return true;

    FileSystem fileSystem = getFileSystem( job.getConfiguration() );

    try
      {
      return fileSystem.delete( getPath(), true );
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
  public boolean pathExists( Job job ) throws IOException
    {
    return getFileSystem( job.getConfiguration() ).exists( getPath() );
    }

  @Override
  public long getPathModified( Job job ) throws IOException
    {
    FileStatus fileStatus = getFileSystem( job.getConfiguration() ).getFileStatus( getPath() );

    if( !fileStatus.isDir() )
      return fileStatus.getModificationTime();

    makeStatuses( job.getConfiguration() );

    // statuses is empty, return 0
    if( statuses == null || statuses.length == 0 )
      return 0;

    long date = 0;

    // filter out directories as we don't recurse into sub dirs
    for( FileStatus status : statuses )
      {
      if( !status.isDir() )
        date = Math.max( date, status.getModificationTime() );
      }

    return date;
    }

  protected Path getTempPath( Configuration conf )
    {
    String tempDir = conf.get( TEMPORARY_DIRECTORY );

    if( tempDir == null )
      tempDir = conf.get( "hadoop.tmp.dir" );

    return new Path( tempDir );
    }

  protected String makeTemporaryPathDir( String name )
    {
    return name.replaceAll( "[\\W\\s]+", "_" ) + Integer.toString( (int) ( 10000000 * Math.random() ) );
    }

  /**
   * Given a file-system object, it makes an array of paths
   *
   * @param conf of type JobConf
   * @throws IOException on failure
   */
  private void makeStatuses( Configuration conf ) throws IOException
    {
    if( statuses != null )
      return;

    statuses = getFileSystem( conf ).listStatus( getPath() );
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

  public TupleEntryIterator openForRead( FlowContext<Configuration> flowContext ) throws IOException
    {
    return new TupleEntryIterator( getSourceFields(), new HfsIterator( this, flowContext ) );
    }

  public TupleEntryCollector openForWrite( FlowContext<Configuration> flowContext ) throws IOException
    {
    if( flowContext instanceof HadoopFlowProcess )
      return new HadoopOutputCollector( this, (HadoopFlowProcess) flowContext );
    else
      return new HfsCollector( this, flowContext );
    }
  }
