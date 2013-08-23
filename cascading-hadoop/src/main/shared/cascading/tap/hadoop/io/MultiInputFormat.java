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

package cascading.tap.hadoop.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import cascading.CascadingException;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.util.Util;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.jets3t.service.S3ServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class MultiInputFormat accepts multiple InputFormat class declarations allowing a single MR job
 * to read data from incompatible file types.
 */
public class MultiInputFormat implements InputFormat
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( MultiInputFormat.class );

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
      if( fromJob.get( "mapred.input.format.class" ) == null )
        throw new CascadingException( "mapred.input.format.class is required, should be set in source Scheme#sourceConfInit" );

      configs.add( HadoopUtil.getConfig( toJob, fromJob ) );
      Collections.addAll( allPaths, FileInputFormat.getInputPaths( fromJob ) );

      if( !isLocal )
        isLocal = fromJob.get( "mapred.job.tracker" ).equalsIgnoreCase( "local" );
      }

    if( !allPaths.isEmpty() ) // it's possible there aren't any
      FileInputFormat.setInputPaths( toJob, (Path[]) allPaths.toArray( new Path[ allPaths.size() ] ) );

    try
      {
      toJob.set( "cascading.multiinputformats", HadoopUtil.serializeBase64( configs, toJob, true ) );
      }
    catch( IOException exception )
      {
      throw new CascadingException( "unable to pack input formats", exception );
      }

    if( isLocal )
      toJob.set( "mapred.job.tracker", "local" );
    }

  static InputFormat[] getInputFormats( JobConf[] jobConfs )
    {
    InputFormat[] inputFormats = new InputFormat[ jobConfs.length ];

    for( int i = 0; i < jobConfs.length; i++ )
      inputFormats[ i ] = jobConfs[ i ].getInputFormat();

    return inputFormats;
    }

  private List<Map<String, String>> getConfigs( JobConf job ) throws IOException
    {
    return (List<Map<String, String>>)
      HadoopUtil.deserializeBase64( job.get( "cascading.multiinputformats" ), job, ArrayList.class, true );
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
    JobConf[] jobConfs = HadoopUtil.getJobConfs( job, configs );
    InputFormat[] inputFormats = getInputFormats( jobConfs );

    // if only one InputFormat, just return what ever it suggests
    if( inputFormats.length == 1 )
      return collapse( getSplits( inputFormats, jobConfs, new int[]{numSplits} ), configs );

    int[] indexedSplits = new int[ inputFormats.length ];

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
      Map<String, String> config = configs.get( i );

      config.remove( "mapred.input.dir" ); // this is a redundant value, will show up cluster side

      InputSplit[] split = splits[ i ];

      for( int j = 0; j < split.length; j++ )
        splitsList.add( new MultiInputSplit( split[ j ], config ) );
      }

    return splitsList.toArray( new InputSplit[ splitsList.size() ] );
    }

  private InputSplit[][] getSplits( InputFormat[] inputFormats, JobConf[] jobConfs, int[] numSplits ) throws IOException
    {
    InputSplit[][] inputSplits = new InputSplit[ inputFormats.length ][];

    for( int i = 0; i < inputFormats.length; i++ )
      {
      inputSplits[ i ] = inputFormats[ i ].getSplits( jobConfs[ i ], numSplits[ i ] );

      // it's reasonable the split array is empty, but really shouldn't be null
      if( inputSplits[ i ] == null )
        inputSplits[ i ] = new InputSplit[ 0 ];

      for( int j = 0; j < inputSplits[ i ].length; j++ )
        {
        if( inputSplits[ i ][ j ] == null )
          throw new IllegalStateException( "input format: " + inputFormats[ i ].getClass().getName() + ", returned a split array with nulls" );
        }
      }

    return inputSplits;
    }

  private long[] getInputSplitSizes( InputFormat[] inputFormats, JobConf[] jobConfs, int numSplits ) throws IOException
    {
    long[] inputSizes = new long[ inputFormats.length ];

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
    final JobConf currentConf = HadoopUtil.mergeConf( job, multiSplit.config, true );

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
