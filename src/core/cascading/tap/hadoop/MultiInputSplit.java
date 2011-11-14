/*
 * Copyright (c) 2007-2010 Concurrent, Inc. All Rights Reserved.
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

package cascading.tap.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

/** Class MultiInputSplit is used by MultiInputFormat */
public class MultiInputSplit implements InputSplit, JobConfigurable
  {
  private static final Logger LOG = Logger.getLogger( MultiInputSplit.class );

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
    return jobConf.get( "cascading.source.path" );
    }

  public MultiInputSplit( InputSplit inputSplit, Map<String, String> config )
    {
    this.inputSplit = inputSplit;
    this.config = config;
    }

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

    JobConf currentConf = MultiInputFormat.mergeConf( jobConf, config, false );

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
        jobConf.set( "cascading.source.path", path.toString() );

        if( LOG.isInfoEnabled() )
          LOG.info( "current split input path: " + path.toString() );
        }
      }
    }
  }
