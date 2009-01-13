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

import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.Tuple;
import cascading.tuple.TupleIterator;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

/**
 * Class TapIterator is an implementation of {@link TupleIterator}. It is returned by {@link cascading.tap.Tap} instances when
 * opening the taps resource for reading.
 */
public class TapIterator implements TupleIterator
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( TapIterator.class );

  /** Field tap */
  private final Tap tap;
  /** Field inputFormat */
  private InputFormat inputFormat;
  /** Field conf */
  private final JobConf conf;
  /** Field splits */
  private InputSplit[] splits;
  /** Field reader */
  private RecordReader reader;
  /** Field key */
  private Object key;
  /** Field value */
  private Object value;

  /** Field currentSplit */
  private int currentSplit = 0;
  /** Field currentTuple */
  private Tuple currentTuple;
  /** Field complete */
  private boolean complete = false;

  /**
   * Constructor TapIterator creates a new TapIterator instance.
   *
   * @param conf of type JobConf
   * @throws IOException when
   */
  public TapIterator( Tap tap, JobConf conf ) throws IOException
    {
    this.tap = tap;
    this.conf = new JobConf( conf );

    initalize();
    }

  private void initalize() throws IOException
    {
    tap.sourceInit( conf );

    if( !tap.pathExists( conf ) )
      {
      complete = true;
      return;
      }

    inputFormat = conf.getInputFormat();

    if( inputFormat instanceof JobConfigurable )
      ( (JobConfigurable) inputFormat ).configure( conf );

    splits = inputFormat.getSplits( conf, 1 );
    reader = makeReader( currentSplit );
    key = reader.createKey();
    value = reader.createValue();

    if( LOG.isDebugEnabled() )
      {
      LOG.debug( "found splits: " + splits.length );
      LOG.debug( "using key: " + key.getClass().getName() );
      LOG.debug( "using value: " + value.getClass().getName() );
      }
    }

  private RecordReader makeReader( int currentSplit ) throws IOException
    {
    if( LOG.isDebugEnabled() )
      LOG.debug( "reading split: " + currentSplit );

    return inputFormat.getRecordReader( splits[ currentSplit ], conf, Reporter.NULL );
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
      if( reader.next( key, value ) )
        {
        currentTuple = tap.source( key, value );
        getNextTuple(); // handles case where currentTuple is returned null from the source
        }
      else if( currentSplit < splits.length - 1 )
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
