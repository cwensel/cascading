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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cascading.CascadingException;
import cascading.util.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;
import org.jets3t.service.S3ServiceException;

/**
 * Class MultiInputFormat accepts multiple InputFormat class declarations allowing a single MR job
 * to read data from incompatible file types.
 */
public class MultiInputFormat extends InputFormat
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( MultiInputFormat.class );

  /**
   * Used to set the current JobConf with all sub jobs configurations.
   *
   * @param toJob
   * @param fromJobs
   */
  public static void addInputFormat( Job toJob, Configuration... fromJobs )
    {
    toJob.setInputFormatClass( MultiInputFormat.class );
    List<Map<String, String>> configs = new ArrayList<Map<String, String>>();
    List<Path> allPaths = new ArrayList<Path>();

    boolean isLocal = false;

    try
      {
      for( Configuration fromJob : fromJobs )
        {
        configs.add( getConfig( toJob, fromJob ) );
        Collections.addAll( allPaths, FileInputFormat.getInputPaths( new Job( fromJob ) ) );

        if( !isLocal )
          isLocal = fromJob.get( "mapred.job.tracker" ).equalsIgnoreCase( "local" );
        }

      FileInputFormat.setInputPaths( toJob, (Path[]) allPaths.toArray( new Path[allPaths.size()] ) );
      }
    catch( IOException exception )
      {
      throw new CascadingException( "unable to handle input formats", exception );
      }

    try
      {
      toJob.getConfiguration().set( "cascading.multiinputformats", Util.serializeBase64( configs ) );
      }
    catch( IOException exception )
      {
      throw new CascadingException( "unable to pack input formats", exception );
      }

    if( isLocal )
      toJob.getConfiguration().set( "mapred.job.tracker", "local" );
    }

  public static Map<String, String> getConfig( Job toJob, Configuration fromJob )
    {
    Map<String, String> configs = new HashMap<String, String>();

    for( Map.Entry<String, String> entry : fromJob )
      configs.put( entry.getKey(), entry.getValue() );

    for( Map.Entry<String, String> entry : toJob.getConfiguration() )
      {
      String value = configs.get( entry.getKey() );

      if( entry.getValue() == null )
        continue;

      if( value == null && entry.getValue() == null )
        configs.remove( entry.getKey() );

      if( value != null && value.equals( entry.getValue() ) )
        configs.remove( entry.getKey() );

      configs.remove( "mapred.working.dir" );
      }

    return configs;
    }

  public static Configuration[] getConfigurations( JobContext job, List<Map<String, String>> configs )
    {
    Configuration[] jobConfs = new Configuration[configs.size()];

    for( int i = 0; i < jobConfs.length; i++ )
      jobConfs[ i ] = mergeConf( job.getConfiguration(), configs.get( i ), false );

    return jobConfs;
    }

  static Configuration mergeConf( Configuration job, Map<String, String> config, boolean directly )
    {
    Configuration currentConf = directly ? job : new Configuration( job );

    for( String key : config.keySet() )
      {
      if( LOG.isDebugEnabled() )
        LOG.debug( "merging key: " + key + " value: " + config.get( key ) );

      currentConf.set( key, config.get( key ) );
      }

    return currentConf;
    }

  static InputFormat[] getInputFormats( Configuration[] jobConfs )
    {
    InputFormat[] inputFormats = new InputFormat[jobConfs.length];

    try
      {
      for( int i = 0; i < jobConfs.length; i++ )
        {
        Class<? extends InputFormat<?, ?>> type = new Job( jobConfs[ i ] ).getInputFormatClass();
        inputFormats[ i ] = ReflectionUtils.newInstance( type, jobConfs[ i ] );
        }
      }
    catch( IOException exception )
      {
      throw new CascadingException( "unable to construct a job", exception );
      }
    catch( ClassNotFoundException exception )
      {
      throw new CascadingException( "unable to load class", exception );
      }

    return inputFormats;
    }

  private List<Map<String, String>> getConfigs( Configuration job ) throws IOException
    {
    return (List<Map<String, String>>) Util.deserializeBase64( job.get( "cascading.multiinputformats" ) );
    }

  public void validateInput( Configuration job ) throws IOException
    {
    // do nothing, is deprecated
    }

  /**
   * Method getSplits delegates to the appropriate InputFormat.
   *
   * @param job of type JobConf
   * @return InputSplit[]
   * @throws IOException when
   */
  @Override
  public List getSplits( JobContext job ) throws IOException, InterruptedException
    {
    List<Map<String, String>> configs = getConfigs( job.getConfiguration() );
    Configuration[] jobConfs = getConfigurations( job, configs );
    InputFormat[] inputFormats = getInputFormats( jobConfs );

    // if only one InputFormat, just return what ever it suggests
    if( inputFormats.length == 1 )
      return collapse( getSplits( inputFormats, jobConfs ), configs );

    return collapse( getSplits( inputFormats, jobConfs ), configs );
    }

  private List collapse( List<InputSplit>[] splits, List<Map<String, String>> configs )
    {
    List<InputSplit> splitsList = new ArrayList<InputSplit>();

    for( int i = 0; i < splits.length; i++ )
      {
      List<InputSplit> splitList = splits[ i ];

      for( int j = 0; j < splitList.size(); j++ )
        splitsList.add( new MultiInputSplit( splitList.get( j ), configs.get( i ) ) );
      }

    return splitsList;
    }

  private List<InputSplit>[] getSplits( InputFormat[] inputFormats, Configuration[] jobConfs ) throws IOException, InterruptedException
    {
    List<InputSplit>[] inputSplits = new List[inputFormats.length];

    for( int i = 0; i < inputFormats.length; i++ )
      inputSplits[ i ] = inputFormats[ i ].getSplits( new Job( jobConfs[ i ] ) );

    return inputSplits;
    }

  /**
   * Method getRecordReader delegates to the appropriate InputFormat.
   *
   * @param split of type InputSplit
   * @param job   of type JobConf
   * @return RecordReader
   * @throws IOException when
   */
  @Override
  public RecordReader createRecordReader( InputSplit split, final TaskAttemptContext job ) throws IOException, InterruptedException
    {
    final MultiInputSplit multiSplit = (MultiInputSplit) split;
    final Configuration currentConf = mergeConf( job.getConfiguration(), multiSplit.config, true );

    try
      {
      return Util.retry( LOG, 3, 20, "unable to get record reader", new Util.RetryOperator<RecordReader>()
      {

      @Override
      public RecordReader operate() throws Exception
        {
        TaskAttemptContext context = new TaskAttemptContext( currentConf, job.getTaskAttemptID() );
        Class<? extends InputFormat<?, ?>> type = new Job( currentConf ).getInputFormatClass();
        return ReflectionUtils.newInstance( type, currentConf ).createRecordReader( multiSplit.inputSplit, context );
        }

      @Override
      public boolean rethrow( Exception exception )
        {
        return !( exception.getCause() instanceof S3ServiceException );
        }
      } );
      }
    catch( Exception exception )
      {
      if( exception instanceof RuntimeException )
        throw (RuntimeException) exception;
      else
        throw (IOException) exception;
      }
    }
  }
