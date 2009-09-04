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

package cascading.tap.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;

/** Class MultiInputSplit is used by MultiInputFormat */
public class MultiInputSplit extends InputSplit implements Configurable, Writable
  {
  /** Field jobConf */
  private transient Configuration conf;
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
   * @return
   */
  public static String getCurrentTapSourcePath( Configuration jobConf )
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

  @Override
  public void setConf( Configuration conf )
    {
    this.conf = conf;
    }

  @Override
  public Configuration getConf()
    {
    return conf;
    }

  public long getLength() throws IOException, InterruptedException
    {
    return inputSplit.getLength();
    }

  public String[] getLocations() throws IOException, InterruptedException
    {
    return inputSplit.getLocations();
    }

  public void write( DataOutput out ) throws IOException
    {
    out.writeUTF( inputSplit.getClass().getName() );

    String[] keys = config.keySet().toArray( new String[config.size()] );
    String[] values = new String[keys.length];

    for( int i = 0; i < keys.length; i++ )
      values[ i ] = config.get( keys[ i ] );

    WritableUtils.writeStringArray( out, keys );
    WritableUtils.writeStringArray( out, values );

    ( (Writable) inputSplit ).write( out );
    }

  public void readFields( DataInput in ) throws IOException
    {
    String splitType = in.readUTF();
    config = new HashMap<String, String>();

    String[] keys = WritableUtils.readStringArray( in );
    String[] values = WritableUtils.readStringArray( in );

    for( int i = 0; i < keys.length; i++ )
      config.put( keys[ i ], values[ i ] );

    MultiInputFormat.mergeConf( conf, config, true );

    try
      {
      inputSplit = (InputSplit) ReflectionUtils.newInstance( conf.getClassByName( splitType ), conf );
      }
    catch( ClassNotFoundException exp )
      {
      throw new IOException( "Split class " + splitType + " not found" );
      }

    ( (Writable) inputSplit ).readFields( in );

    if( inputSplit instanceof FileSplit )
      {
      Path path = ( (FileSplit) inputSplit ).getPath();

      if( path != null )
        conf.set( "cascading.source.path", path.toString() );
      }
    }

  }
