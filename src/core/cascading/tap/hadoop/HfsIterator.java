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
import java.util.List;
import java.util.ListIterator;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowContext;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.Tuple;
import cascading.tuple.TupleIterator;
import cascading.tuple.TupleListCollector;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

/**
 * Class TapIterator is an implementation of {@link TupleIterator}. It is returned by {@link cascading.tap.Tap} instances when
 * opening the taps resource for reading.
 */
public class HfsIterator implements TupleIterator
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( HfsIterator.class );

  /** Field tap */
  private final Tap tap;
  /** Field inputFormat */
  private InputFormat inputFormat;
  /** Field conf */
  private final Configuration conf;
  /** Field splits */
  private List<InputSplit> splits;
  /** Field reader */
  private RecordReader reader;

  /** Field currentSplit */
  private int currentSplit = 0;
  /** Field tupleListCollector */
  private TupleListCollector tupleListCollector;
  /** Field tupleListIterator */
  private ListIterator<Tuple> tupleListIterator;
  /** Field currentTuple */
  private Tuple currentTuple;
  /** Field complete */
  private boolean complete = false;

  /**
   * Constructor TapIterator creates a new TapIterator instance.
   *
   * @param flowContext
   * @throws IOException when
   */
  public HfsIterator( Tap tap, HadoopFlowContext flowContext ) throws IOException
    {
    this.tap = tap;
    this.conf = new Configuration( ( (Flow) flowContext ).getConfiguration() );

    initalize();
    }

  private void initalize() throws IOException
    {
    Job job = new Job( conf );

    tap.sourceInit( job );
    tupleListCollector = new TupleListCollector( tap.getSourceFields() );
    tupleListIterator = tupleListCollector.listIterator(); // is empty

    if( !tap.pathExists( job ) )
      {
      complete = true;
      return;
      }

    try
      {
      inputFormat = ReflectionUtils.newInstance( job.getInputFormatClass(), conf );
      }
    catch( ClassNotFoundException exception )
      {

      }

    if( inputFormat instanceof Configurable )
      ( (Configurable) inputFormat ).setConf( conf );

    try
      {
      splits = inputFormat.getSplits( job );
      }
    catch( InterruptedException exception )
      {

      }

    if( splits.size() == 0 )
      {
      complete = true;
      return;
      }

    reader = makeReader( currentSplit );

    if( LOG.isDebugEnabled() )
      LOG.debug( "found splits: " + splits.size() );
    }

  private RecordReader makeReader( int currentSplit ) throws IOException
    {
    if( LOG.isDebugEnabled() )
      LOG.debug( "reading split: " + currentSplit );

    try
      {
      return inputFormat.createRecordReader( splits.get( currentSplit ), Hadoop19TapUtil.getAttemptContext( conf ) );
      }
    catch( InterruptedException exception )
      {
      throw new RuntimeException( exception );
      }
    }

  /**
   * Method hasNext returns true if there more {@link Tuple} instances available.
   *
   * @return boolean
   */
  public boolean hasNext()
    {
    getNextTuple();

    return !complete;
    }

  /**
   * Method next returns the next {@link Tuple}.
   *
   * @return Tuple
   */
  public Tuple next()
    {
    try
      {
      getNextTuple();

      return currentTuple;
      }
    finally
      {
      currentTuple = null;
      }
    }

  private void getNextTuple()
    {
    if( currentTuple != null || reader == null )
      return;

    try
      {
      if( tupleListIterator.hasNext() )
        {
        currentTuple = tupleListIterator.next();
        getNextTuple(); // handles case where currentTuple is returned null from the source
        }
      else if( reader.nextKeyValue() )
        {
        tap.source( new Tuple( reader.getCurrentKey(), reader.getCurrentValue() ), tupleListCollector );
        tupleListIterator = tupleListCollector.listIterator();
        getNextTuple();
        }
      else if( currentSplit < splits.size() - 1 )
          {
          reader.close();
          reader = makeReader( ++currentSplit );
          getNextTuple();
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
    catch( InterruptedException exception )
      {
      throw new RuntimeException( exception );
      }
    }

  public void remove()
    {
    throw new UnsupportedOperationException( "unimplemented" );
    }

  public void close()
    {
    try
      {
      if( reader != null )
        reader.close();
      }
    catch( IOException exception )
      {
      LOG.warn( "exception closing iteraor", exception );
      }
    }
  }
