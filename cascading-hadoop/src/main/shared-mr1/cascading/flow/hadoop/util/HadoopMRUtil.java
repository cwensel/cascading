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
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import cascading.util.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class HadoopMRUtil
  {
  private static final Logger LOG = LoggerFactory.getLogger( HadoopMRUtil.class );

  public static String writeStateToDistCache( JobConf conf, String id, String kind, String stepState )
    {
    if( Util.isEmpty( stepState ) )
      return null;

    LOG.info( "writing step state to dist cache, too large for job conf, size: {}", stepState.length() );

    String statePath = Hfs.getTempPath( conf ) + "/" + kind + "-state-" + id;

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

  public static String readStateFromDistCache( JobConf jobConf, String id, String kind ) throws IOException
    {
    Path[] files = DistributedCache.getLocalCacheFiles( jobConf );

    Path stepStatePath = null;

    for( Path file : files )
      {
      if( !file.toString().contains( kind + "-state-" + id ) )
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
      try
        {
        if( reader != null )
          reader.close();
        }
      catch( IOException exception )
        {
        LOG.warn( "error closing state path reader", exception );
        }
      }
    }

  /**
   * Add to class path.
   *
   * @param config    the config
   * @param classpath the classpath
   */
  public static Map<Path, Path> addToClassPath( Configuration config, List<String> classpath )
    {
    if( classpath == null )
      return null;

    // given to fully qualified
    Map<String, Path> localPaths = new HashMap<String, Path>();
    Map<String, Path> remotePaths = new HashMap<String, Path>();

    HadoopUtil.resolvePaths( config, classpath, null, null, localPaths, remotePaths );

    try
      {
      LocalFileSystem localFS = HadoopUtil.getLocalFS( config );

      for( String path : localPaths.keySet() )
        {
        // only add local if no remote
        if( remotePaths.containsKey( path ) )
          continue;

        Path artifact = localPaths.get( path );

        DistributedCache.addFileToClassPath( artifact.makeQualified( localFS ), config );
        }

      FileSystem defaultFS = HadoopUtil.getDefaultFS( config );

      for( String path : remotePaths.keySet() )
        {
        // always add remote
        Path artifact = remotePaths.get( path );

        DistributedCache.addFileToClassPath( artifact.makeQualified( defaultFS ), config );
        }
      }
    catch( IOException exception )
      {
      throw new FlowException( "unable to set distributed cache paths", exception );
      }

    return HadoopUtil.getCommonPaths( localPaths, remotePaths );
    }

  public static boolean hasReducer( JobConf jobConf )
    {
    return jobConf.getReducerClass() != null;
    }
  }
