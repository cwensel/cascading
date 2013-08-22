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

package cascading.tap.hadoop;

import java.beans.ConstructorProperties;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.tap.MultiSourceTap;
import cascading.tap.TapException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

/**
 * Class GlobHfs is a type of {@link cascading.tap.MultiSourceTap} that accepts Hadoop style 'file globing' expressions so
 * multiple files that match the given pattern may be used as the input sources for a given {@link cascading.flow.Flow}.
 * <p/>
 * See {@link FileSystem#globStatus(org.apache.hadoop.fs.Path)} for details on the globing syntax. But in short
 * it is similar to standard regular expressions except alternation is done via {foo,bar} instead of (foo|bar).
 * <p/>
 * Note that a {@link cascading.flow.Flow} sourcing from GlobHfs is not currently compatible with the {@link cascading.cascade.Cascade}
 * scheduler. GlobHfs expects the files and paths to exist so the wildcards can be resolved into concrete values so
 * that the scheduler can order the Flows properly.
 * <p/>
 * Note that globing can match files or directories. It may consume less resources to match directories and let
 * Hadoop include all sub-files immediately contained in the directory instead of enumerating every individual file.
 * Ending the glob path with a {@code /} should match only directories.
 *
 * @see Hfs
 * @see cascading.tap.MultiSourceTap
 * @see FileSystem
 */
public class GlobHfs extends MultiSourceTap<Hfs, JobConf, RecordReader>
  {
  /** Field pathPattern */
  private final String pathPattern;
  /** Field pathFilter */
  private final PathFilter pathFilter;

  /**
   * Constructor GlobHfs creates a new GlobHfs instance.
   *
   * @param scheme      of type Scheme
   * @param pathPattern of type String
   */
  @ConstructorProperties({"scheme", "pathPattern"})
  public GlobHfs( Scheme<JobConf, RecordReader, ?, ?, ?> scheme, String pathPattern )
    {
    this( scheme, pathPattern, null );
    }

  /**
   * Constructor GlobHfs creates a new GlobHfs instance.
   *
   * @param scheme      of type Scheme
   * @param pathPattern of type String
   * @param pathFilter  of type PathFilter
   */
  @ConstructorProperties({"scheme", "pathPattern", "pathFilter"})
  public GlobHfs( Scheme<JobConf, RecordReader, ?, ?, ?> scheme, String pathPattern, PathFilter pathFilter )
    {
    super( scheme );
    this.pathPattern = pathPattern;
    this.pathFilter = pathFilter;
    }

  @Override
  public String getIdentifier()
    {
    return pathPattern;
    }

  @Override
  protected Hfs[] getTaps()
    {
    return initTapsInternal( new JobConf() );
    }

  private Hfs[] initTapsInternal( JobConf conf )
    {
    if( taps != null )
      return taps;

    try
      {
      taps = makeTaps( conf );
      }
    catch( IOException exception )
      {
      throw new TapException( "unable to resolve taps for globing path: " + pathPattern );
      }

    return taps;
    }

  private Hfs[] makeTaps( JobConf conf ) throws IOException
    {
    FileStatus[] statusList;

    Path path = new Path( pathPattern );

    FileSystem fileSystem = path.getFileSystem( conf );

    if( pathFilter == null )
      statusList = fileSystem.globStatus( path );
    else
      statusList = fileSystem.globStatus( path, pathFilter );

    if( statusList == null || statusList.length == 0 )
      throw new TapException( "unable to find paths matching path pattern: " + pathPattern );

    List<Hfs> notEmpty = new ArrayList<Hfs>();

    for( int i = 0; i < statusList.length; i++ )
      {
      // remove empty files. some hadoop versions return non-zero for dirs
      // so this jives with the expectations set in the above javadoc
      if( statusList[ i ].isDir() || statusList[ i ].getLen() != 0 )
        notEmpty.add( new Hfs( getScheme(), statusList[ i ].getPath().toString() ) );
      }

    if( notEmpty.isEmpty() )
      throw new TapException( "all paths matching path pattern are zero length and not directories: " + pathPattern );

    return notEmpty.toArray( new Hfs[ notEmpty.size() ] );
    }

  @Override
  public void sourceConfInit( FlowProcess<JobConf> process, JobConf conf )
    {
    initTapsInternal( conf );
    super.sourceConfInit( null, conf );
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( object == null || getClass() != object.getClass() )
      return false;

    GlobHfs globHfs = (GlobHfs) object;

    // do not compare tap arrays, these values should be sufficient to show identity
    if( getScheme() != null ? !getScheme().equals( globHfs.getScheme() ) : globHfs.getScheme() != null )
      return false;
    if( pathFilter != null ? !pathFilter.equals( globHfs.pathFilter ) : globHfs.pathFilter != null )
      return false;
    if( pathPattern != null ? !pathPattern.equals( globHfs.pathPattern ) : globHfs.pathPattern != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = pathPattern != null ? pathPattern.hashCode() : 0;
    result = 31 * result + ( pathFilter != null ? pathFilter.hashCode() : 0 );
    return result;
    }

  @Override
  public String toString()
    {
    return "GlobHfs[" + pathPattern + ']';
    }
  }
