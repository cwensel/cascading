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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cascading.CascadingException;
import cascading.util.Util;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.jets3t.service.S3ServiceException;

/**
 * Class MultiInputFormat accepts multiple InputFormat class declarations allowing a single MR job
 * to read data from incompatible file types.
 */
public class MultiInputFormat implements InputFormat
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( MultiInputFormat.class );

  /**
   * Used to set the current JobConf with all sub jobs configurations.
   *
   * @param toJob
   * @param fromJobs
   */
  public static void addInputFormat( JobConf toJob, JobConf... fromJobs )
    {
    toJob.setInputFormat( MultiInputFormat.class );
    List<Map<String, String>> configs = new ArrayList<Map<String, String>>();
    List<Path> allPaths = new ArrayList<Path>();

    boolean isLocal = false;

    for( JobConf fromJob : fromJobs )
      {
      configs.add( getConfig( toJob, fromJob ) );
      Collections.addAll( allPaths, FileInputFormat.getInputPaths( fromJob ) );

      if( !isLocal )
        isLocal = fromJob.get( "mapred.job.tracker" ).equalsIgnoreCase( "local" );
      }

    FileInputFormat.setInputPaths( toJob, (Path[]) allPaths.toArray( new Path[allPaths.size()] ) );

    try
      {
      toJob.set( "cascading.multiinputformats", Util.serializeBase64( configs ) );
      }
    catch( IOException exception )
      {
      throw new CascadingException( "unable to pack input formats", exception );
      }

    if( isLocal )
      toJob.set( "mapred.job.tracker", "local" );
    }

  public static Map<String, String> getConfig( JobConf toJob, JobConf fromJob )
    {
    Map<String, String> configs = new HashMap<String, String>();

    for( Map.Entry<String, String> entry : fromJob )
      configs.put( entry.getKey(), entry.getValue() );

    for( Map.Entry<String, String> entry : toJob )
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

  public static JobConf[] getJobConfs( JobConf job, List<Map<String, String>> configs )
    {
    JobConf[] jobConfs = new JobConf[configs.size()];

    for( int i = 0; i < jobConfs.length; i++ )
      jobConfs[ i ] = mergeConf( job, configs.get( i ), false );

    return jobConfs;
    }

  static JobConf mergeConf( JobConf job, Map<String, String> config, boolean directly )
    {
    JobConf currentConf = directly ? job : new JobConf( job );

    for( String key : config.keySet() )
      {
      if( LOG.isDebugEnabled() )
        LOG.debug( "merging key: " + key + " value: " + config.get( key ) );

      currentConf.set( key, config.get( key ) );
      }

    return currentConf;
    }

  static InputFormat[] getInputFormats( JobConf[] jobConfs )
    {
    InputFormat[] inputFormats = new InputFormat[jobConfs.length];

    for( int i = 0; i < jobConfs.length; i++ )
      inputFormats[ i ] = jobConfs[ i ].getInputFormat();

    return inputFormats;
    }

  private List<Map<String, String>> getConfigs( JobConf job ) throws IOException
    {
    return (List<Map<String, String>>) Util.deserializeBase64( job.get( "cascading.multiinputformats" ) );
    }

  public void validateInput( JobConf job ) throws IOException
    {
    // do nothing, is deprecated
    }

  /**
   * Method getSplits delegates to the appropriate InputFormat.
   *
   * @param job       of type JobConf
   * @param numSplits of type int
   * @return InputSplit[]
   * @throws IOException when
   */
  public InputSplit[] getSplits( JobConf job, int numSplits ) throws IOException
    {
    numSplits = numSplits == 0 ? 1 : numSplits;

    List<Map<String, String>> configs = getConfigs( job );
    JobConf[] jobConfs = getJobConfs( job, configs );
    InputFormat[] inputFormats = getInputFormats( jobConfs );

    // if only one InputFormat, just return what ever it suggests
    if( inputFormats.length == 1 )
      return collapse( getSplits( inputFormats, jobConfs, new int[]{numSplits} ), configs );

    int[] indexedSplits = new int[inputFormats.length];

    // if we need only a few, the return one for each
    if( numSplits <= inputFormats.length )
      {
      Arrays.fill( indexedSplits, 1 );
      return collapse( getSplits( inputFormats, jobConfs, indexedSplits ), configs );
      }

    // attempt to get splits proportionally sized per input format
    long[] inputSplitSizes = getInputSplitSizes( inputFormats, jobConfs, numSplits );
    long totalSplitSize = sum( inputSplitSizes );

    if( totalSplitSize == 0 )
      {
      Arrays.fill( indexedSplits, 1 );
      return collapse( getSplits( inputFormats, jobConfs, indexedSplits ), configs );
      }

    for( int i = 0; i < inputSplitSizes.length; i++ )
      {
      int useSplits = (int) Math.ceil( (double) numSplits * inputSplitSizes[ i ] / (double) totalSplitSize );
      indexedSplits[ i ] = useSplits == 0 ? 1 : useSplits;
      }

    return collapse( getSplits( inputFormats, jobConfs, indexedSplits ), configs );
    }

  private long sum( long[] inputSizes )
    {
    long size = 0;

    for( long inputSize : inputSizes )
      size += inputSize;

    return size;
    }

  private InputSplit[] collapse( InputSplit[][] splits, List<Map<String, String>> configs )
    {
    List<InputSplit> splitsList = new ArrayList<InputSplit>();

    for( int i = 0; i < splits.length; i++ )
      {
      InputSplit[] split = splits[ i ];

      for( int j = 0; j < split.length; j++ )
        splitsList.add( new MultiInputSplit( split[ j ], configs.get( i ) ) );
      }

    return splitsList.toArray( new InputSplit[splitsList.size()] );
    }

  private InputSplit[][] getSplits( InputFormat[] inputFormats, JobConf[] jobConfs, int[] numSplits ) throws IOException
    {
    InputSplit[][] inputSplits = new InputSplit[inputFormats.length][];

    for( int i = 0; i < inputFormats.length; i++ )
      inputSplits[ i ] = inputFormats[ i ].getSplits( jobConfs[ i ], numSplits[ i ] );

    return inputSplits;
    }

  private long[] getInputSplitSizes( InputFormat[] inputFormats, JobConf[] jobConfs, int numSplits ) throws IOException
    {
    long[] inputSizes = new long[inputFormats.length];

    for( int i = 0; i < inputFormats.length; i++ )
      {
      InputFormat inputFormat = inputFormats[ i ];
      InputSplit[] splits = inputFormat.getSplits( jobConfs[ i ], numSplits );

      inputSizes[ i ] = splits.length;
      }

    return inputSizes;
    }

  /**
   * Method getRecordReader delegates to the appropriate InputFormat.
   *
   * @param split    of type InputSplit
   * @param job      of type JobConf
   * @param reporter of type Reporter
   * @return RecordReader
   * @throws IOException when
   */
  public RecordReader getRecordReader( InputSplit split, JobConf job, final Reporter reporter ) throws IOException
    {
    final MultiInputSplit multiSplit = (MultiInputSplit) split;
    final JobConf currentConf = mergeConf( job, multiSplit.config, true );

    try
      {
      return Util.retry( LOG, 3, 20, "unable to get record reader", new Util.RetryOperator<RecordReader>()
      {

      @Override
      public RecordReader operate() throws Exception
        {
        return currentConf.getInputFormat().getRecordReader( multiSplit.inputSplit, currentConf, reporter );
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
