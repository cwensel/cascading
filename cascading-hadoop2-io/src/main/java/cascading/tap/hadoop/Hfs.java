/*
 * Copyright (c) 2016-2018 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading.tap.hadoop;

import java.beans.ConstructorProperties;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.flow.FlowRuntimeProps;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tap.hadoop.io.CombineFileRecordReaderWrapper;
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeCollector;
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeIterator;
import cascading.tap.type.FileType;
import cascading.tap.type.TapWith;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.hadoop.TupleSerialization;
import cascading.util.LazyIterable;
import cascading.util.Util;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileRecordReader;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class Hfs is the base class for all Hadoop file system access. Hfs may only be used with the
 * Hadoop {@link cascading.flow.FlowConnector} sub-classes when creating Hadoop executable {@link cascading.flow.Flow}
 * instances.
 * <p>
 * Paths typically should point to a directory, where in turn all the "part" files immediately in that directory will
 * be included. This is the practice Hadoop expects. Sub-directories are not included and typically result in a failure.
 * <p>
 * To include sub-directories, Hadoop supports "globing". Globing is a frustrating feature and is supported more
 * robustly by {@link GlobHfs} and less so by Hfs.
 * <p>
 * Hfs will accept {@code /*} (wildcard) paths, but not all convenience methods like
 * {@code jobConf.getSize} will behave properly or reliably. Nor can the Hfs instance
 * with a wildcard path be used as a sink to write data.
 * <p>
 * In those cases use GlobHfs since it is a sub-class of {@link cascading.tap.MultiSourceTap}.
 * <p>
 * Optionally use {@link Dfs} or {@link Lfs} for resources specific to Hadoop Distributed file system or
 * the Local file system, respectively. Using Hfs is the best practice when possible, Lfs and Dfs are conveniences.
 * <p>
 * Use the Hfs class if the 'kind' of resource is unknown at design time. To use, prefix a scheme to the 'stringPath'. Where
 * {@code hdfs://...} will denote Dfs, and {@code file://...} will denote Lfs.
 * <p>
 * Call {@link HfsProps#setTemporaryDirectory(java.util.Map, String)} to use a different temporary file directory path
 * other than the current Hadoop default path.
 * <p>
 * By default Cascading on Hadoop will assume any source or sink Tap using the {@code file://} URI scheme
 * intends to read files from the local client filesystem (for example when using the {@code Lfs} Tap) where the Hadoop
 * job jar is started. Subsequently Cascading will force any MapReduce jobs reading or writing to {@code file://} resources
 * to run in Hadoop "standalone mode" so that the file can be read.
 * <p>
 * To change this behavior, {@link HfsProps#setLocalModeScheme(java.util.Map, String)} to set a different scheme value,
 * or to "none" to disable entirely for the case the file to be read is available on every Hadoop processing node
 * in the exact same path.
 * <p>
 * When using a MapReduce planner, Hfs can optionally combine multiple small files (or a series of small "blocks") into
 * larger "splits". This reduces the number of resulting map tasks created by Hadoop and can improve application
 * performance.
 * <p>
 * This is enabled by calling {@link HfsProps#setUseCombinedInput(boolean)} to {@code true}. By default, merging
 * or combining splits into large ones is disabled.
 * <p>
 * Apache Tez planner does not require this setting, it is supported by default and enabled by the application manager.
 */
public class Hfs extends Tap<Configuration, RecordReader, OutputCollector> implements FileType<Configuration>, TapWith<Configuration, RecordReader, OutputCollector>
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( Hfs.class );

  /** Field stringPath */
  protected String stringPath;
  /** Field uriScheme */
  transient URI uriScheme;
  /** Field path */
  transient Path path;
  /** Field paths */
  private transient FileStatus[] statuses; // only used by getModifiedTime

  private transient String cachedPath = null;

  private static final PathFilter HIDDEN_FILES_FILTER = path ->
  {
  String name = path.getName();

  if( name.isEmpty() ) // should never happen
    return true;

  char first = name.charAt( 0 );

  return first != '_' && first != '.';
  };

  protected static String getLocalModeScheme( Configuration conf, String defaultValue )
    {
    return conf.get( HfsProps.LOCAL_MODE_SCHEME, defaultValue );
    }

  protected static boolean getUseCombinedInput( Configuration conf )
    {
    boolean combineEnabled = conf.getBoolean( "cascading.hadoop.hfs.combine.files", false );

    if( conf.get( FlowRuntimeProps.COMBINE_SPLITS ) == null && !combineEnabled )
      return false;

    if( !combineEnabled ) // enable if set in FlowRuntimeProps
      combineEnabled = conf.getBoolean( FlowRuntimeProps.COMBINE_SPLITS, false );

    String platform = conf.get( "cascading.flow.platform", "" );

    // only supported by these platforms
    if( platform.equals( "hadoop" ) || platform.equals( "hadoop2-mr1" ) )
      return combineEnabled;

    // we are on a platform that supports combining, just not through the combiner
    // do not enable it here locally
    if( conf.get( FlowRuntimeProps.COMBINE_SPLITS ) != null )
      return false;

    if( combineEnabled && !Boolean.getBoolean( "cascading.hadoop.hfs.combine.files.warned" ) )
      {
      LOG.warn( "'cascading.hadoop.hfs.combine.files' has been set to true, but is unsupported by this platform: {}, will be ignored to prevent failures", platform );
      System.setProperty( "cascading.hadoop.hfs.combine.files.warned", "true" );
      }

    return false;
    }

  protected static boolean getCombinedInputSafeMode( Configuration conf )
    {
    return conf.getBoolean( "cascading.hadoop.hfs.combine.safemode", true );
    }

  protected Hfs()
    {
    }

  @ConstructorProperties({"scheme"})
  protected Hfs( Scheme<Configuration, RecordReader, OutputCollector, ?, ?> scheme )
    {
    super( scheme );
    }

  /**
   * Constructor Hfs creates a new Hfs instance.
   *
   * @param scheme     of type Scheme
   * @param stringPath of type String
   */
  @ConstructorProperties({"scheme", "stringPath"})
  public Hfs( Scheme<Configuration, RecordReader, OutputCollector, ?, ?> scheme, String stringPath )
    {
    super( scheme );
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
  public Hfs( Scheme<Configuration, RecordReader, OutputCollector, ?, ?> scheme, String stringPath, SinkMode sinkMode )
    {
    super( scheme, sinkMode );
    setStringPath( stringPath );
    }

  /**
   * Constructor Hfs creates a new Hfs instance.
   *
   * @param scheme of type Scheme
   * @param path   of type Path
   */
  @ConstructorProperties({"scheme", "path"})
  public Hfs( Scheme<Configuration, RecordReader, OutputCollector, ?, ?> scheme, Path path )
    {
    super( scheme );
    setStringPath( path.toString() );
    }

  /**
   * Constructor Hfs creates a new Hfs instance.
   *
   * @param scheme   of type Scheme
   * @param path     of type Path
   * @param sinkMode of type SinkMode
   */
  @ConstructorProperties({"scheme", "path", "sinkMode"})
  public Hfs( Scheme<Configuration, RecordReader, OutputCollector, ?, ?> scheme, Path path, SinkMode sinkMode )
    {
    super( scheme, sinkMode );
    setStringPath( path.toString() );
    }

  @Override
  public TapWith<Configuration, RecordReader, OutputCollector> withChildIdentifier( String identifier )
    {
    Path path = new Path( identifier );

    if( !path.toString().startsWith( getPath().toString() ) )
      path = new Path( getPath(), path );

    return create( getScheme(), path, getSinkMode() );
    }

  @Override
  public TapWith<Configuration, RecordReader, OutputCollector> withScheme( Scheme<Configuration, RecordReader, OutputCollector, ?, ?> scheme )
    {
    return create( scheme, getPath(), getSinkMode() );
    }

  @Override
  public TapWith<Configuration, RecordReader, OutputCollector> withSinkMode( SinkMode sinkMode )
    {
    return create( getScheme(), getPath(), sinkMode );
    }

  protected TapWith<Configuration, RecordReader, OutputCollector> create( Scheme<Configuration, RecordReader, OutputCollector, ?, ?> scheme, Path path, SinkMode sinkMode )
    {
    try
      {
      return Util.newInstance( getClass(), new Object[]{scheme, path, sinkMode} );
      }
    catch( CascadingException exception )
      {
      throw new TapException( "unable to create a new instance of: " + getClass().getName(), exception );
      }
    }

  protected void setStringPath( String stringPath )
    {
    this.stringPath = Util.normalizeUrl( stringPath );
    }

  protected void setUriScheme( URI uriScheme )
    {
    this.uriScheme = uriScheme;
    }

  public URI getURIScheme( Configuration jobConf )
    {
    if( uriScheme != null )
      return uriScheme;

    uriScheme = makeURIScheme( jobConf );

    return uriScheme;
    }

  protected URI makeURIScheme( Configuration configuration )
    {
    try
      {
      URI uriScheme;

      LOG.debug( "handling path: {}", stringPath );

      URI uri = new Path( stringPath ).toUri(); // safer URI parsing
      String schemeString = uri.getScheme();
      String authority = uri.getAuthority();

      LOG.debug( "found scheme: {}, authority: {}", schemeString, authority );

      if( schemeString != null && authority != null )
        uriScheme = new URI( schemeString + "://" + uri.getAuthority() );
      else if( schemeString != null )
        uriScheme = new URI( schemeString + ":///" );
      else
        uriScheme = getDefaultFileSystemURIScheme( configuration );

      LOG.debug( "using uri scheme: {}", uriScheme );

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
   * @param configuration of type JobConf
   * @return URI
   */
  public URI getDefaultFileSystemURIScheme( Configuration configuration )
    {
    return getDefaultFileSystem( configuration ).getUri();
    }

  protected FileSystem getDefaultFileSystem( Configuration configuration )
    {
    try
      {
      return FileSystem.get( configuration );
      }
    catch( IOException exception )
      {
      throw new TapException( "unable to get handle to underlying filesystem", exception );
      }
    }

  protected FileSystem getFileSystem( Configuration configuration )
    {
    URI scheme = getURIScheme( configuration );

    try
      {
      return FileSystem.get( scheme, configuration );
      }
    catch( IOException exception )
      {
      throw new TapException( "unable to get handle to get filesystem for: " + scheme.getScheme(), exception );
      }
    }

  @Override
  public String getIdentifier()
    {
    if( cachedPath == null )
      cachedPath = getPath().toString();

    return cachedPath;
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
  public String getFullIdentifier( Configuration conf )
    {
    return getPath().makeQualified( getFileSystem( conf ) ).toString();
    }

  @Override
  public void sourceConfInit( FlowProcess<? extends Configuration> process, Configuration conf )
    {
    String fullIdentifier = getFullIdentifier( conf );

    applySourceConfInitIdentifiers( process, conf, fullIdentifier );

    verifyNoDuplicates( conf );
    }

  protected static void verifyNoDuplicates( Configuration conf )
    {
    Path[] inputPaths = FileInputFormat.getInputPaths( HadoopUtil.asJobConfInstance( conf ) );
    Set<Path> paths = new HashSet<Path>( (int) ( inputPaths.length / .75f ) );

    for( Path inputPath : inputPaths )
      {
      if( !paths.add( inputPath ) )
        throw new TapException( "may not add duplicate paths, found: " + inputPath );
      }
    }

  protected void applySourceConfInitIdentifiers( FlowProcess<? extends Configuration> process, Configuration conf, final String... fullIdentifiers )
    {
    sourceConfInitAddInputPaths( conf, new LazyIterable<String, Path>( fullIdentifiers )
      {
      @Override
      protected Path convert( String next )
        {
        return new Path( next );
        }
      } );

    sourceConfInitComplete( process, conf );
    }

  protected void sourceConfInitAddInputPaths( Configuration conf, Iterable<Path> qualifiedPaths )
    {
    HadoopUtil.addInputPaths( conf, qualifiedPaths );

    for( Path qualifiedPath : qualifiedPaths )
      {
      boolean stop = !makeLocal( conf, qualifiedPath, "forcing job to stand-alone mode, via source: " );

      if( stop )
        break;
      }
    }

  @Deprecated
  protected void sourceConfInitAddInputPath( Configuration conf, Path qualifiedPath )
    {
    HadoopUtil.addInputPath( conf, qualifiedPath );

    makeLocal( conf, qualifiedPath, "forcing job to stand-alone mode, via source: " );
    }

  protected void sourceConfInitComplete( FlowProcess<? extends Configuration> process, Configuration conf )
    {
    super.sourceConfInit( process, conf );

    TupleSerialization.setSerializations( conf ); // allows Hfs to be used independent of Flow

    // use CombineFileInputFormat if that is enabled
    handleCombineFileInputFormat( conf );
    }

  /**
   * Based on the configuration, handles and sets {@link CombineFileInputFormat} as the input
   * format.
   */
  private void handleCombineFileInputFormat( Configuration conf )
    {
    // if combining files, override the configuration to use CombineFileInputFormat
    if( !getUseCombinedInput( conf ) )
      return;

    // get the prescribed individual input format from the underlying scheme so it can be used by CombinedInputFormat
    String individualInputFormat = conf.get( "mapred.input.format.class" );

    if( individualInputFormat == null )
      throw new TapException( "input format is missing from the underlying scheme" );

    if( individualInputFormat.equals( CombinedInputFormat.class.getName() ) &&
      conf.get( CombineFileRecordReaderWrapper.INDIVIDUAL_INPUT_FORMAT ) == null )
      throw new TapException( "the input format class is already the combined input format but the underlying input format is missing" );

    // if safe mode is on (default) throw an exception if the InputFormat is not a FileInputFormat, otherwise log a
    // warning and don't use the CombineFileInputFormat
    boolean safeMode = getCombinedInputSafeMode( conf );

    if( !FileInputFormat.class.isAssignableFrom( conf.getClass( "mapred.input.format.class", null ) ) )
      {
      if( safeMode )
        throw new TapException( "input format must be of type org.apache.hadoop.mapred.FileInputFormat, got: " + individualInputFormat );
      else
        LOG.warn( "not combining input splits with CombineFileInputFormat, {} is not of type org.apache.hadoop.mapred.FileInputFormat.", individualInputFormat );
      }
    else
      {
      // set the underlying individual input format
      conf.set( CombineFileRecordReaderWrapper.INDIVIDUAL_INPUT_FORMAT, individualInputFormat );

      // override the input format class
      conf.setClass( "mapred.input.format.class", CombinedInputFormat.class, InputFormat.class );
      }
    }

  @Override
  public void sinkConfInit( FlowProcess<? extends Configuration> process, Configuration conf )
    {
    Path qualifiedPath = new Path( getFullIdentifier( conf ) );

    HadoopUtil.setOutputPath( conf, qualifiedPath );
    super.sinkConfInit( process, conf );

    makeLocal( conf, qualifiedPath, "forcing job to stand-alone mode, via sink: " );

    TupleSerialization.setSerializations( conf ); // allows Hfs to be used independent of Flow
    }

  private boolean makeLocal( Configuration conf, Path qualifiedPath, String infoMessage )
    {
    // don't change the conf or log any messages if running cluster side
    if( HadoopUtil.isInflow( conf ) )
      return false;

    String scheme = getLocalModeScheme( conf, "file" );

    if( !HadoopUtil.isLocal( conf ) && qualifiedPath.toUri().getScheme().equalsIgnoreCase( scheme ) )
      {
      if( LOG.isInfoEnabled() )
        LOG.info( infoMessage + toString() );

      HadoopUtil.setLocal( conf ); // force job to run locally

      return false; // only need to set local once
      }

    return true;
    }

  @Override
  public TupleEntryIterator openForRead( FlowProcess<? extends Configuration> flowProcess, RecordReader input ) throws IOException
    {
    // input may be null when this method is called on the client side or cluster side when accumulating
    // for a HashJoin
    return new HadoopTupleEntrySchemeIterator( flowProcess, this, input );
    }

  @Override
  public TupleEntryCollector openForWrite( FlowProcess<? extends Configuration> flowProcess, OutputCollector output ) throws IOException
    {
    resetFileStatuses();

    // output may be null when this method is called on the client side or cluster side when creating
    // side files with the PartitionTap
    return new HadoopTupleEntrySchemeCollector( flowProcess, this, output );
    }

  @Override
  public boolean createResource( Configuration conf ) throws IOException
    {
    if( LOG.isDebugEnabled() )
      LOG.debug( "making dirs: {}", getFullIdentifier( conf ) );

    return getFileSystem( conf ).mkdirs( getPath() );
    }

  @Override
  public boolean deleteResource( Configuration conf ) throws IOException
    {
    String fullIdentifier = getFullIdentifier( conf );

    return deleteFullIdentifier( conf, fullIdentifier );
    }

  private boolean deleteFullIdentifier( Configuration conf, String fullIdentifier ) throws IOException
    {
    if( LOG.isDebugEnabled() )
      LOG.debug( "deleting: {}", fullIdentifier );

    resetFileStatuses();

    Path fullPath = new Path( fullIdentifier );

    // do not delete the root directory
    if( fullPath.depth() == 0 )
      return true;

    FileSystem fileSystem = getFileSystem( conf );

    try
      {
      return fileSystem.delete( fullPath, true );
      }
    catch( NullPointerException exception )
      {
      // hack to get around npe thrown when fs reaches root directory
      // removes coupling to the new aws hadoop artifacts that may not be deployed
      if( !( fileSystem.getClass().getSimpleName().equals( "NativeS3FileSystem" ) ) )
        throw exception;
      }

    return true;
    }

  public boolean deleteChildResource( FlowProcess<? extends Configuration> flowProcess, String childIdentifier ) throws IOException
    {
    return deleteChildResource( flowProcess.getConfig(), childIdentifier );
    }

  public boolean deleteChildResource( Configuration conf, String childIdentifier ) throws IOException
    {
    resetFileStatuses();

    Path childPath = new Path( childIdentifier ).makeQualified( getFileSystem( conf ) );

    if( !childPath.toString().startsWith( getFullIdentifier( conf ) ) )
      return false;

    return deleteFullIdentifier( conf, childPath.toString() );
    }

  @Override
  public boolean resourceExists( Configuration conf ) throws IOException
    {
    // unfortunately getFileSystem( conf ).exists( getPath() ); does not account for "/*" etc
    // nor is there an more efficient means to test for existence
    FileStatus[] fileStatuses = getFileSystem( conf ).globStatus( getPath() );

    return fileStatuses != null && fileStatuses.length > 0;
    }

  @Override
  public boolean isDirectory( FlowProcess<? extends Configuration> flowProcess ) throws IOException
    {
    return isDirectory( flowProcess.getConfig() );
    }

  @Override
  public boolean isDirectory( Configuration conf ) throws IOException
    {
    if( !resourceExists( conf ) )
      return false;

    return getFileSystem( conf ).getFileStatus( getPath() ).isDir();
    }

  @Override
  public long getSize( FlowProcess<? extends Configuration> flowProcess ) throws IOException
    {
    return getSize( flowProcess.getConfig() );
    }

  @Override
  public long getSize( Configuration conf ) throws IOException
    {
    if( !resourceExists( conf ) )
      return 0;

    FileStatus fileStatus = getFileStatus( conf );

    if( fileStatus.isDir() )
      return 0;

    return getFileSystem( conf ).getFileStatus( getPath() ).getLen();
    }

  /**
   * Method getBlockSize returns the {@code blocksize} specified by the underlying file system for this resource.
   *
   * @param flowProcess
   * @return long
   * @throws IOException when
   */
  public long getBlockSize( FlowProcess<? extends Configuration> flowProcess ) throws IOException
    {
    return getBlockSize( flowProcess.getConfig() );
    }

  /**
   * Method getBlockSize returns the {@code blocksize} specified by the underlying file system for this resource.
   *
   * @param conf of JobConf
   * @return long
   * @throws IOException when
   */
  public long getBlockSize( Configuration conf ) throws IOException
    {
    if( !resourceExists( conf ) )
      return 0;

    FileStatus fileStatus = getFileStatus( conf );

    if( fileStatus.isDir() )
      return 0;

    return fileStatus.getBlockSize();
    }

  /**
   * Method getReplication returns the {@code replication} specified by the underlying file system for
   * this resource.
   *
   * @param flowProcess
   * @return int
   * @throws IOException when
   */
  public int getReplication( FlowProcess<? extends Configuration> flowProcess ) throws IOException
    {
    return getReplication( flowProcess.getConfig() );
    }

  /**
   * Method getReplication returns the {@code replication} specified by the underlying file system for
   * this resource.
   *
   * @param conf of JobConf
   * @return int
   * @throws IOException when
   */
  public int getReplication( Configuration conf ) throws IOException
    {
    if( !resourceExists( conf ) )
      return 0;

    FileStatus fileStatus = getFileStatus( conf );

    if( fileStatus.isDir() )
      return 0;

    return fileStatus.getReplication();
    }

  @Override
  public String[] getChildIdentifiers( FlowProcess<? extends Configuration> flowProcess ) throws IOException
    {
    return getChildIdentifiers( flowProcess.getConfig(), 1, false );
    }

  @Override
  public String[] getChildIdentifiers( Configuration conf ) throws IOException
    {
    return getChildIdentifiers( conf, 1, false );
    }

  @Override
  public String[] getChildIdentifiers( FlowProcess<? extends Configuration> flowProcess, int depth, boolean fullyQualified ) throws IOException
    {
    return getChildIdentifiers( flowProcess.getConfig(), depth, fullyQualified );
    }

  @Override
  public String[] getChildIdentifiers( Configuration conf, int depth, boolean fullyQualified ) throws IOException
    {
    if( !resourceExists( conf ) )
      return new String[ 0 ];

    if( depth == 0 && !fullyQualified )
      return new String[]{getIdentifier()};

    String fullIdentifier = getFullIdentifier( conf );

    int trim = fullyQualified ? 0 : fullIdentifier.length() + 1;

    Set<String> results = new LinkedHashSet<String>();

    getChildPaths( conf, results, trim, new Path( fullIdentifier ), depth );

    return results.toArray( new String[ results.size() ] );
    }

  private void getChildPaths( Configuration conf, Set<String> results, int trim, Path path, int depth ) throws IOException
    {
    if( depth == 0 )
      {
      String substring = path.toString().substring( trim );
      String identifier = getIdentifier();

      if( identifier == null || identifier.isEmpty() )
        results.add( new Path( substring ).toString() );
      else
        results.add( new Path( identifier, substring ).toString() );

      return;
      }

    FileStatus[] statuses = getFileSystem( conf ).listStatus( path, HIDDEN_FILES_FILTER );

    if( statuses == null )
      return;

    for( FileStatus fileStatus : statuses )
      getChildPaths( conf, results, trim, fileStatus.getPath(), depth - 1 );
    }

  @Override
  public long getModifiedTime( Configuration conf ) throws IOException
    {
    if( !resourceExists( conf ) )
      return 0;

    FileStatus fileStatus = getFileStatus( conf );

    if( !fileStatus.isDir() )
      return fileStatus.getModificationTime();

    // todo: this should ignore the _temporary path, or not cache if found in the array
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

  public FileStatus getFileStatus( Configuration conf ) throws IOException
    {
    return getFileSystem( conf ).getFileStatus( getPath() );
    }

  public static Path getTempPath( Configuration conf )
    {
    String tempDir = conf.get( HfsProps.TEMPORARY_DIRECTORY );

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

    return name.replaceAll( "[\\W\\s]+", "_" ) + Util.createUniqueID();
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

  /**
   * Method resetFileStatuses removes the status cache, if any.
   */
  public void resetFileStatuses()
    {
    statuses = null;
    }

  /** Combined input format that uses the underlying individual input format to combine multiple files into a single split. */
  static class CombinedInputFormat extends CombineFileInputFormat implements Configurable
    {
    private Configuration conf;

    public RecordReader getRecordReader( InputSplit split, JobConf job, Reporter reporter ) throws IOException
      {
      return new CombineFileRecordReader( job, (CombineFileSplit) split, reporter, CombineFileRecordReaderWrapper.class );
      }

    @Override
    public void setConf( Configuration conf )
      {
      this.conf = conf;

      // set the aliased property value, if zero, the super class will look up the hadoop property
      setMaxSplitSize( conf.getLong( "cascading.hadoop.hfs.combine.max.size", 0 ) );
      }

    @Override
    public Configuration getConf()
      {
      return conf;
      }
    }
  }
