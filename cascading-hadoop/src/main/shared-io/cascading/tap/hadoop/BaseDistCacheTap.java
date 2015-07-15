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

package cascading.tap.hadoop;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.tap.DecoratorTap;
import cascading.tap.MultiSourceTap;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.TupleEntryIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public abstract class BaseDistCacheTap extends DecoratorTap<Void, Configuration, RecordReader, OutputCollector>
  {
  /** logger. */
  private static final Logger LOG = LoggerFactory.getLogger( BaseDistCacheTap.class );

  public BaseDistCacheTap( Tap<Configuration, RecordReader, OutputCollector> original )
    {
    super( original );
    }

  @Override
  public void sourceConfInit( FlowProcess<? extends Configuration> process, Configuration conf )
    {
    if( HadoopUtil.isLocal( conf ) ||
      Tap.id( this ).equals( conf.get( "cascading.node.source" ) ) ||
      Tap.id( this ).equals( conf.get( "cascading.step.source" ) ) )
      {
      LOG.info( "can't use distributed cache. reading '{}' from hdfs", super.getIdentifier() );
      super.sourceConfInit( process, conf );
      return;
      }
    try
      {
      registerHfs( process, conf, getHfs() );
      }
    catch( IOException exception )
      {
      throw new TapException( exception );
      }
    }

  @Override
  public TupleEntryIterator openForRead( FlowProcess<? extends Configuration> flowProcess, RecordReader input ) throws IOException
    {
    // always read via Hadoop FileSystem if in standalone/local mode, or if an RecordReader is provided
    if( HadoopUtil.isLocal( flowProcess.getConfig() ) || input != null )
      {
      LOG.info( "delegating to parent" );
      return super.openForRead( flowProcess, input );
      }

    Path[] cachedFiles = getLocalCacheFiles( flowProcess );

    if( cachedFiles == null || cachedFiles.length == 0 )
      return super.openForRead( flowProcess, null );

    List<Path> paths = new ArrayList<>();
    List<Tap> taps = new ArrayList<>();

    if( isSimpleGlob() )
      {
      FileSystem fs = FileSystem.get( flowProcess.getConfig() );
      FileStatus[] statuses = fs.globStatus( getHfs().getPath() );

      for( FileStatus status : statuses )
        paths.add( status.getPath() );
      }
    else
      {
      paths.add( getHfs().getPath() );
      }

    for( Path pathToFind : paths )
      {
      for( Path path : cachedFiles )
        {
        if( path.toString().endsWith( pathToFind.getName() ) )
          {
          LOG.info( "found {} in distributed cache", path );
          taps.add( new Lfs( getScheme(), path.toString() ) );
          }
        }
      }

    if( paths.isEmpty() ) // not in cache, read from HDFS
      {
      LOG.info( "could not find files in local resource path. delegating to parent: {}", super.getIdentifier() );
      return super.openForRead( flowProcess, input );
      }

    return new MultiSourceTap( taps.toArray( new Tap[ taps.size() ] ) ).openForRead( flowProcess, input );
    }

  private void registerHfs( FlowProcess<? extends Configuration> process, Configuration conf, Hfs hfs ) throws IOException
    {
    if( isSimpleGlob() )
      {
      FileSystem fs = FileSystem.get( conf );
      FileStatus[] statuses = fs.globStatus( getHfs().getPath() );

      if( statuses == null || statuses.length == 0 )
        throw new TapException( String.format( "glob expression %s does not match any files on the filesystem", getHfs().getPath() ) );

      for( FileStatus fileStatus : statuses )
        registerURI( conf, fileStatus.getPath() );
      }
    else
      {
      registerURI( conf, hfs.getPath() );
      }

    hfs.sourceConfInitComplete( process, conf );
    }

  private void registerURI( Configuration conf, Path path )
    {
    URI uri = path.toUri();
    LOG.info( "adding {} to local resource configuration ", uri );
    addLocalCacheFiles( conf, uri );
    }

  private Hfs getHfs()
    {
    return (Hfs) getOriginal();
    }

  private boolean isSimpleGlob()
    {
    return getHfs().getIdentifier().contains( "*" );
    }

  protected abstract Path[] getLocalCacheFiles( FlowProcess<? extends Configuration> flowProcess ) throws IOException;

  protected abstract void addLocalCacheFiles( Configuration conf, URI uri );
  }
