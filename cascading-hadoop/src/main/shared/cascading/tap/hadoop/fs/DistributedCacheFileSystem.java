/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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

package cascading.tap.hadoop.fs;

import cascading.tap.Tap;
import cascading.util.Util;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.regex.Pattern;

/**
 * FileSystem API to DistributedCache-based Tap.
 * Since DistributedCache symlinks all files into container's working dir
 * only 1-level path nesting is supported.
 *
 * A cached URI has the form:
 *  &lt;scheme&gt;://&lt;authority&gt;/&lt;path&gt;#&lt;tapId&gt;-&lt;cdcfs&gt;-&lt;linkuuid&gt;
 *
 * DCFS recognizes the following paths:
 * <ol>
 *  <li>cdcfs:///&lt;tapId&gt;-&lt;cdcfs&gt;-&lt;linkuuid&gt; - treated as a file.</li>
 *  <li>cdcfs:///&lt;tapId&gt; - treated as directory if a &lt;tapId&gt;-&lt;cdcfs&gt;-&lt;linkuuid&gt; exists
 *    <ul>
 *      <li>listStatus returns an array of the corresponding RLFS FileStaus objects</li>
 *      <li>getFileStatus returns a virtual directory file status</li>
 *    </ul>
 *  </li>
 * </ol>
 *
 */
public class DistributedCacheFileSystem extends FileSystem
  {
  private static final Log LOG = LogFactory.getLog( DistributedCacheFileSystem.class );
  public static final String DCFS_SCHEME = "cdcfs"; // cascading distributed cache file system
  public static final String DCFS_IMPL = String.format( "fs.%s.impl", DCFS_SCHEME );

  public static final Path DCFS_ROOT = new Path( DCFS_SCHEME + ":///" );
  private static final String CACHED_TAPS = "cascading.tap.hadoop.cached.";

  public static final String UUID_REGEX_STR = "[A-F0-9]{32}";

  private static final Pattern DIR_PATTERN = Pattern.compile(UUID_REGEX_STR);
  private static final Pattern FILE_PATTERN = Pattern.compile(
      UUID_REGEX_STR + "-" + DCFS_SCHEME + "-" + UUID_REGEX_STR);
  private static final Pattern PATH_PATTERN = Pattern.compile(
      UUID_REGEX_STR + "(-" + DCFS_SCHEME + "-" + UUID_REGEX_STR + ")?");

  public static class ReadOnlyFileSystem extends UnsupportedOperationException
    {}

  private final FileSystem fs;

  /**
   * A helpler to access protected FIF.listStatus
   */
  private static class FIFListStatusHelper extends FileInputFormat {
    @Override
    public RecordReader getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException
      {
      throw new UnsupportedOperationException();
      }

    private FileStatus[] delegateListStatus(JobConf jobConf) throws IOException
      {
      return listStatus(jobConf);
      }
  }

  public DistributedCacheFileSystem()
    {
    super();
    fs = new RawLocalFileSystem();
    }


  /**
   * Prepare source tap to be added to DistributedCache
   *
   * @param tap
   * @param accumulatedConf
   * @param stepConf
   */
  public static void distCacheTap(Tap tap, JobConf accumulatedConf, JobConf stepConf)
    {
    String tapId = Tap.id( tap );
    Path[] p = FileInputFormat.getInputPaths( accumulatedConf );

    // no FIF input?
    if( p.length == 0 )
      return;

    String cachedPaths = CACHED_TAPS + tapId;

    // already processed the tap?
    if( stepConf.get( cachedPaths ) != null )
      return;
    if( LOG.isDebugEnabled() )
      LOG.debug(cachedPaths + ": adding " + Arrays.toString( p ) );

    // build csv list
    StringBuilder sb = new StringBuilder();
    sb.append( p[ 0 ].toString() );
    for( int i = 1; i < p.length; i++ )
      sb.append( ',' ).append( p[ i ].toString() );

    // remember paths to add to dist cache before launching the job
    stepConf.setStrings( cachedPaths, sb.toString() );

    // replace the real paths by a virtual dist cache "dir" tapid
    FileInputFormat.setInputPaths( accumulatedConf, DCFS_ROOT + tapId );
    accumulatedConf.setClass( DCFS_IMPL, DistributedCacheFileSystem.class, FileSystem.class );
    }

  /**
   * Add FIF input files to DistributedCache for the Tap prepared by distCacheTap
   *
   * @param sourceTaps
   * @param currentConf
   * @throws IOException
   */
  public static void populateDistCache(Collection<Tap> sourceTaps, JobConf currentConf) throws IOException
    {
    URI[] cachedUris = DistributedCache.getCacheFiles( currentConf );
    FIFListStatusHelper fifHelper = new FIFListStatusHelper();
    JobConf fifConf = new JobConf( currentConf );

    tap_loop: for( Tap tap : sourceTaps )
      {
      String tapId = Tap.id( tap );
      String cachedTapConfKey = CACHED_TAPS + tapId;

      // tap needs caching?
      String csvPaths = currentConf.get( cachedTapConfKey );
      if( csvPaths == null )
        continue tap_loop;

      // avoid duplicates in DC
      if( cachedUris != null )
        for( URI u : cachedUris )
          if( u.getFragment() != null && u.getFragment().startsWith( tapId ) )
            continue tap_loop;

      // FIF figures out the paths involved (including recursion).
      FileInputFormat.setInputPaths( fifConf, csvPaths );
      FileStatus[] fifFiles = fifHelper.delegateListStatus( fifConf );
      if ( fifFiles == null )
        return;

      for( FileStatus fileStatus : fifFiles )
        {
        // <scheme>://<authority>/<path>#<tapId>-<cdcfs>-<linkuuid>
        String uriStr = String.format("%s#%s-%s-%s",
            fileStatus.getPath().toUri(),    // <scheme>://<authority>/<path>
            tapId,
            DCFS_SCHEME,
            Util.createUniqueID());

        if( LOG.isDebugEnabled() )
          LOG.debug( "populateDistCache adding: " + uriStr );

        try
          {
          DistributedCache.addCacheFile( new URI(uriStr), currentConf );
          } catch (URISyntaxException infeasible)
          {
          throw new IOException(uriStr + ": Invalid uri formed", infeasible);
          }
        }
      }
      DistributedCache.createSymlink(currentConf); // hadoop1
    }

  @Override
  public void initialize(URI name, Configuration conf) throws IOException
    {
    super.initialize(name, conf);
    fs.initialize(name, conf);
    }

  @Override
  public URI getUri()
    {
    return DCFS_ROOT.toUri();
    }

  /**
   * Open is provided only for completeness and debugging. MapReduce will not
   * normally use it. The flow in MR is to call IF.getSplits which in turn for
   * file-based IF boils down to getting/listing FileStatus'es of via FS.
   * This file system will delegate these calls to RawLocalFileSystem. Hence,
   * the subsequent open calls on splits will be made against RLFS.
   *
   * @param f
   * @param bufferSize
   * @return
   * @throws IOException
   */
  @Override
  public FSDataInputStream open( Path f, int bufferSize ) throws IOException
    {
    if( shouldDelegate( f ) )
      return fs.open( f );

    Path qualifiedPath = makeQualified( f );
    if( qualifiedPath.getParent() != null && qualifiedPath.getParent().getParent() == null )
      {
      String pathName = qualifiedPath.getName();
      if( FILE_PATTERN.matcher( pathName ).matches() )
        return fs.open( new Path( fs.getWorkingDirectory(), pathName ) );
      }

    throw new FileNotFoundException( f + ": not found." );
    }

  private boolean shouldDelegate( Path f )
    {
    if(! PATH_PATTERN.matcher( f.getName()).matches() )
      throw new IllegalArgumentException( " Not owned by dcfs " );

    return fs.getUri().getScheme().equals( f.toUri().getScheme() );
    }

  @Override
  public FSDataOutputStream create(
    Path f,
    FsPermission permission,
    boolean overwrite,
    int bufferSize,
    short replication,
    long blockSize,
    Progressable progress)
    throws IOException
    {
    throw new ReadOnlyFileSystem();
    }

  @Override
  public FSDataOutputStream append(
    Path f,
    int bufferSize,
    Progressable progress )
    throws IOException
    {
    throw new ReadOnlyFileSystem();
    }

  @Override
  public boolean rename( Path src, Path dst ) throws IOException
    {
    throw new ReadOnlyFileSystem();
    }

  @Override
  public boolean delete(Path f) throws IOException
    {
    return delete( f, false );
    }

  @Override
  public boolean delete( Path f, boolean recursive ) throws IOException
    {
    throw new ReadOnlyFileSystem();
    }

  /**
   * If f is a cached file, return local file status, otherwise empty array
   *
   * @param f
   * @return array of file statuses
   * @throws java.io.IOException
   */
  @Override
  public FileStatus[] listStatus( Path f ) throws IOException
    {
    if( LOG.isDebugEnabled() )
      LOG.debug( "Calling listStatus on: " + f, new Throwable( "stacktace" ) );

    if( shouldDelegate(f) )
      return fs.listStatus( f );

    Path qualifiedPath = makeQualified( f );

    // only 1-level virtual directory <tapid>-cdcfs- supported
    if( qualifiedPath.getParent() != null && qualifiedPath.getParent().getParent() == null )
      {
      String pathName = qualifiedPath.getName();
      FileStatus[] result = fs.globStatus( new Path ( fs.getWorkingDirectory(), pathName + "*" ) );
      if( LOG.isDebugEnabled() )
        LOG.debug( "listStatus " + f + ": " + Arrays.toString( result ) );
      return result;
      }

    throw new FileNotFoundException( f + ": Only 1-level directories supported." );
    }

  @Override
  public void setWorkingDirectory( Path new_dir )
    {
    // nop
    }

  @Override
  public Path getWorkingDirectory()
    {
    return DCFS_ROOT;
    }

  @Override
  public boolean mkdirs( Path f, FsPermission permission ) throws IOException
    {
    throw new ReadOnlyFileSystem();
    }

  @Override
  public FileStatus getFileStatus( Path f ) throws IOException
    {
    if( LOG.isDebugEnabled() )
      LOG.debug( "Calling getFileStatus on: " + f, new Throwable( "stacktace" ) );

    if( shouldDelegate( f ) )
      return fs.getFileStatus( f );

    Path qualifiedPath = makeQualified( f );

    if(    qualifiedPath.getParent() != null
        && qualifiedPath.getParent().getParent() == null )
      {
      String pathName = qualifiedPath.getName();

      // return directory status without looking up the contents of DC
      if( DIR_PATTERN.matcher( pathName ).matches() )
        return makeDirStatus( qualifiedPath );

      // shouldDelegate already checked for valid path pattern. Hence, not dir implies file
      FileStatus fileStatus = fs.getFileStatus(new Path(fs.getWorkingDirectory(), pathName));
      if( LOG.isDebugEnabled() )
        LOG.debug( "getFileStatus " +f + ": " + fileStatus );
      return fileStatus;
      }
    throw new FileNotFoundException( qualifiedPath + ": not found." );
    }

  @Override
  public Path makeQualified( Path f )
    {
    if( f.toUri().getScheme() != null )
      return f;
    else
      return f.isAbsolute()
          ? new Path( DCFS_SCHEME + "://" + f)
          : new Path( DCFS_ROOT, f);
    }

  private FileStatus makeDirStatus(Path qualifiedPath)
    {
    return new FileStatus( 0, true, 0, 0, 0, qualifiedPath );
    }

  }
