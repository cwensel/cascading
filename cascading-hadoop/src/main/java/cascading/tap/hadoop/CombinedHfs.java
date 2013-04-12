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

import java.io.IOException;
import java.util.Arrays;

import cascading.flow.FlowProcess;
import cascading.tap.MultiSourceTap;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tap.hadoop.io.CombineFileRecordReaderWrapper;
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeIterator;
import cascading.tuple.TupleEntryIterator;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileRecordReader;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

import static java.util.Arrays.copyOf;

/**
 * CombinedHfs can be used in lieu of a single or multiple {@link Hfs} instances when a number of input files
 * should be combined to form bigger hadoop splits. Like {@link Hfs}, CombinedHfs may only be used with the
 * {@link cascading.flow.hadoop.HadoopFlowConnector} when creating Hadoop executable {@link cascading.flow.Flow}
 * instances.
 * <p/>
 * A CombinedHfs is a wrapper around {@link Hfs}. The input format set by the Hfs scheme is used to handle
 * individual files.
 *
 * @see org.apache.hadoop.mapred.lib.CombineFileInputFormat
 */
public class CombinedHfs extends MultiSourceTap<Hfs, JobConf, RecordReader>
  {
  /**
   * Constructor for creating a CombinedHfs instance.
   *
   * @param taps of type Hfs...
   */
  public CombinedHfs( Hfs... taps )
    {
    super( taps );
    }

  @Override
  public TupleEntryIterator openForRead( FlowProcess<JobConf> flowProcess, RecordReader input ) throws IOException
    {
    // use the hadoop tuple entry iterator directly
    return new HadoopTupleEntrySchemeIterator( flowProcess, this, input );
    }

  @Override
  public void sourceConfInit( FlowProcess<JobConf> process, JobConf conf )
    {
    // let the children initialize conf first
    super.sourceConfInit( process, conf );

    // get the prescribed individual input format from the underlying scheme so it can be used by CombinedInputFormat
    String individualInputFormat = conf.get( "mapred.input.format.class" );

    if( individualInputFormat == null )
      throw new TapException( "input format is missing from the underlying scheme" );

    if( individualInputFormat.equals( CombinedInputFormat.class.getName() ) )
      throw new TapException( "the input format class is already the combined input format!" );

    conf.set( CombineFileRecordReaderWrapper.INDIVIDUAL_INPUT_FORMAT, individualInputFormat );

    // override the input format class
    conf.setInputFormat( CombinedInputFormat.class );
    }

  @Override
  public String toString()
    {
    Tap[] printableTaps = getTaps();

    if( printableTaps == null )
      return "CombinedHfs[none]";

    String printedTaps;

    if( printableTaps.length > 10 )
      printedTaps = Arrays.toString( copyOf( printableTaps, 10 ) ) + ",...";
    else
      printedTaps = Arrays.toString( printableTaps );

    return "CombinedHfs[" + printableTaps.length + ':' + printedTaps + ']';
    }

  /** Combined input format that uses the underlying individual input format to combine multiple files into a single split. */
  static class CombinedInputFormat extends CombineFileInputFormat
    {
    public RecordReader getRecordReader( InputSplit split, JobConf job, Reporter reporter ) throws IOException
      {
      return new CombineFileRecordReader( job, (CombineFileSplit) split, reporter, CombineFileRecordReaderWrapper.class );
      }
    }
  }
