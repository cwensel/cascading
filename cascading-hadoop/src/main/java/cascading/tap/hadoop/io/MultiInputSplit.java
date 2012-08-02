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

package cascading.tap.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import cascading.flow.hadoop.util.HadoopUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class MultiInputSplit is used by MultiInputFormat */
public class MultiInputSplit implements InputSplit, JobConfigurable
  {
  public static final String CASCADING_SOURCE_PATH = "cascading.source.path";
  private static final Logger LOG = LoggerFactory.getLogger( MultiInputSplit.class );

  /** Field jobConf */
  private transient JobConf jobConf;
  /** Field inputSplit */
  InputSplit inputSplit;
  /** Field config */
  Map<String, String> config;

  /**
   * Method getCurrentTapSourcePath finds and returns the current source Tap filename path, if any.
   * <p/>
   * Use this method inside an Operation to find the current file being processed.
   *
   * @param jobConf
   * @return a String
   */
  public static String getCurrentTapSourcePath( JobConf jobConf )
    {
    return jobConf.get( CASCADING_SOURCE_PATH );
    }

  public MultiInputSplit( InputSplit inputSplit, Map<String, String> config )
    {
    if( inputSplit == null )
      throw new IllegalArgumentException( "input split may not be null" );

    if( config == null )
      throw new IllegalArgumentException( "config may not be null" );

    this.inputSplit = inputSplit;
    this.config = config;
    }

  /**
   * This constructor is used internally by Hadoop. it is expected {@link #configure(org.apache.hadoop.mapred.JobConf)}
   * and {@link #readFields(java.io.DataInput)} are called to properly initialize.
   */
  public MultiInputSplit()
    {
    }

  public void configure( JobConf jobConf )
    {
    this.jobConf = jobConf;
    }

  public long getLength() throws IOException
    {
    return inputSplit.getLength();
    }

  public String[] getLocations() throws IOException
    {
    return inputSplit.getLocations();
    }

  public InputSplit getWrappedInputSplit()
    {
    return inputSplit;
    }

  public void write( DataOutput out ) throws IOException
    {
    out.writeUTF( inputSplit.getClass().getName() );

    String[] keys = config.keySet().toArray( new String[ config.size() ] );
    String[] values = new String[ keys.length ];

    for( int i = 0; i < keys.length; i++ )
      values[ i ] = config.get( keys[ i ] );

    WritableUtils.writeStringArray( out, keys );
    WritableUtils.writeStringArray( out, values );

    inputSplit.write( out );
    }

  public void readFields( DataInput in ) throws IOException
    {
    String splitType = in.readUTF();
    config = new HashMap<String, String>();

    String[] keys = WritableUtils.readStringArray( in );
    String[] values = WritableUtils.readStringArray( in );

    for( int i = 0; i < keys.length; i++ )
      config.put( keys[ i ], values[ i ] );

    JobConf currentConf = HadoopUtil.mergeConf( jobConf, config, false );

    try
      {
      inputSplit = (InputSplit) ReflectionUtils.newInstance( currentConf.getClassByName( splitType ), currentConf );
      }
    catch( ClassNotFoundException exp )
      {
      throw new IOException( "split class " + splitType + " not found" );
      }

    inputSplit.readFields( in );

    if( inputSplit instanceof FileSplit )
      {
      Path path = ( (FileSplit) inputSplit ).getPath();

      if( path != null )
        {
        jobConf.set( CASCADING_SOURCE_PATH, path.toString() );

        LOG.info( "current split input path: {}", path );
        }
      }
    }
  }
