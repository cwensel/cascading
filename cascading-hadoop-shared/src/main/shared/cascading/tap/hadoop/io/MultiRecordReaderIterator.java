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

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.Tuple;
import cascading.util.CloseableIterator;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class TapIterator is an implementation of {@link cascading.util.CloseableIterator}. It is returned by {@link cascading.tap.Tap} instances when
 * opening the taps resource for reading.
 */
public class MultiRecordReaderIterator implements CloseableIterator<RecordReader>
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( MultiRecordReaderIterator.class );

  private final FlowProcess<JobConf> flowProcess;
  /** Field tap */
  private final Tap tap;
  /** Field inputFormat */
  private InputFormat inputFormat;
  /** Field conf */
  private JobConf conf;
  /** Field splits */
  private InputSplit[] splits;
  /** Field reader */
  private RecordReader reader;

  /** Field lastReader */
  private RecordReader lastReader;

  /** Field currentSplit */
  private int currentSplit = 0;
  /** Field complete */
  private boolean complete = false;

  /**
   * Constructor TapIterator creates a new TapIterator instance.
   *
   * @throws IOException when
   */
  public MultiRecordReaderIterator( FlowProcess<JobConf> flowProcess, Tap tap ) throws IOException
    {
    this.flowProcess = flowProcess;
    this.tap = tap;
    this.conf = flowProcess.getConfigCopy();

    initialize();
    }

  private void initialize() throws IOException
    {
    // prevent collisions of configuration properties set client side if now cluster side
    String property = flowProcess.getStringProperty( "cascading.step.accumulated.source.conf." + Tap.id( tap ) );

    if( property == null )
      {
      // default behavior is to accumulate paths, so remove any set prior
      conf = HadoopUtil.removePropertiesFrom( conf, "mapred.input.dir" );
      tap.sourceConfInit( flowProcess, conf );
      }

    inputFormat = conf.getInputFormat();

    if( inputFormat instanceof JobConfigurable )
      ( (JobConfigurable) inputFormat ).configure( conf );

    // do not test for existence, let hadoop decide how to handle the given path
    // this delegates globbing to the inputformat on split generation.
    splits = inputFormat.getSplits( conf, 1 );

    if( splits.length == 0 )
      complete = true;
    }

  private RecordReader makeReader( int currentSplit ) throws IOException
    {
    LOG.debug( "reading split: {}", currentSplit );

    return inputFormat.getRecordReader( splits[ currentSplit ], conf, Reporter.NULL );
    }

  /**
   * Method hasNext returns true if there more {@link Tuple} instances available.
   *
   * @return boolean
   */
  public boolean hasNext()
    {
    getNextReader();

    return !complete;
    }

  /**
   * Method next returns the next {@link Tuple}.
   *
   * @return Tuple
   */
  public RecordReader next()
    {
    if( complete )
      throw new IllegalStateException( "no more values" );

    try
      {
      getNextReader();

      return reader;
      }
    finally
      {
      reader = null;
      }
    }

  private void getNextReader()
    {
    if( complete || reader != null )
      return;

    try
      {
      if( currentSplit < splits.length )
        {
        if( lastReader != null )
          lastReader.close();

        reader = makeReader( currentSplit++ );
        lastReader = reader;
        }
      else
        {
        complete = true;
        }
      }
    catch( IOException exception )
      {
      throw new TapException( "could not get next tuple", exception );
      }
    }

  public void remove()
    {
    throw new UnsupportedOperationException( "unimplemented" );
    }

  @Override
  public void close() throws IOException
    {
    if( lastReader != null )
      lastReader.close();
    }
  }
