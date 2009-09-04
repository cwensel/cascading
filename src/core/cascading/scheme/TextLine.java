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

package cascading.scheme;

import java.io.IOException;

import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tap.hadoop.HadoopEntryCollector;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * A TextLine is a type of {@link Scheme} for plain text files. Files are broken into
 * lines. Either line-feed or carriage-return are used to signal end of line.
 * <p/>
 * By default, this scheme returns a {@link Tuple} with two fields, "offset" and "line". But if
 * a {@link Fields} is passed on the constructor with one field, the return tuples will simply
 * be the "line" value using the given field name.
 */
public class TextLine extends Scheme
  {
  public enum Compress
    {
      DEFAULT, ENABLE, DISABLE
    }

  /** Field serialVersionUID */
  private static final long serialVersionUID = 1L;
  /** Field DEFAULT_SOURCE_FIELDS */
  public static final Fields DEFAULT_SOURCE_FIELDS = new Fields( "offset", "line" );

  /** Field sinkCompression */
  Compress sinkCompression = Compress.DISABLE;

  /**
   * Creates a new TextLine instance that sources "offset" and "line" fields, and sinks all incoming fields, where
   * "offset" is the byte offset in the input file.
   */
  public TextLine()
    {
    super( DEFAULT_SOURCE_FIELDS );
    }

  /**
   * Creates a new TextLine instance that sources "offset" and "line" fields, and sinks all incoming fields, where
   * "offset" is the byte offset in the input file.
   *
   * @param numSinkParts of type int
   */
  public TextLine( int numSinkParts )
    {
    super( DEFAULT_SOURCE_FIELDS, numSinkParts );
    }

  /**
   * Creates a new TextLine instance that sources "offset" and "line" fields, and sinks all incoming fields, where
   * "offset" is the byte offset in the input file.
   *
   * @param sinkCompression of type Compress
   */
  public TextLine( Compress sinkCompression )
    {
    super( DEFAULT_SOURCE_FIELDS );

    this.sinkCompression = sinkCompression;
    }

  /**
   * Creates a new TextLine instance. If sourceFields has one field, only the text line will be returned in the
   * subsequent tuples.
   *
   * @param sourceFields the source fields for this scheme
   * @param sinkFields   the sink fields for this scheme
   */
  public TextLine( Fields sourceFields, Fields sinkFields )
    {
    super( sourceFields, sinkFields );

    if( sourceFields.size() < 1 || sourceFields.size() > 2 )
      throw new IllegalArgumentException( "this scheme requires either one or two source fields, given [" + sourceFields + "]" );
    }

  /**
   * Creates a new TextLine instance. If sourceFields has one field, only the text line will be returned in the
   * subsequent tuples.
   *
   * @param sourceFields the source fields for this scheme
   * @param sinkFields   the sink fields for this scheme
   * @param numSinkParts of type int
   */
  public TextLine( Fields sourceFields, Fields sinkFields, int numSinkParts )
    {
    super( sourceFields, sinkFields, numSinkParts );

    if( sourceFields.size() < 1 || sourceFields.size() > 2 )
      throw new IllegalArgumentException( "this scheme requires either one or two source fields, given [" + sourceFields + "]" );
    }

  /**
   * Constructor TextLine creates a new TextLine instance. If sourceFields has one field, only the text line will be returned in the
   * subsequent tuples.
   *
   * @param sourceFields    of type Fields
   * @param sinkFields      of type Fields
   * @param sinkCompression of type Compress
   */
  public TextLine( Fields sourceFields, Fields sinkFields, Compress sinkCompression )
    {
    super( sourceFields, sinkFields );

    this.sinkCompression = sinkCompression;

    if( sourceFields.size() < 1 || sourceFields.size() > 2 )
      throw new IllegalArgumentException( "this scheme requires either one or two source fields, given [" + sourceFields + "]" );
    }

  /**
   * Constructor TextLine creates a new TextLine instance. If sourceFields has one field, only the text line will be returned in the
   * subsequent tuples.
   *
   * @param sourceFields    of type Fields
   * @param sinkFields      of type Fields
   * @param sinkCompression of type Compress
   * @param numSinkParts    of type int
   */
  public TextLine( Fields sourceFields, Fields sinkFields, Compress sinkCompression, int numSinkParts )
    {
    super( sourceFields, sinkFields, numSinkParts );

    this.sinkCompression = sinkCompression;

    if( sourceFields.size() < 1 || sourceFields.size() > 2 )
      throw new IllegalArgumentException( "this scheme requires either one or two source fields, given [" + sourceFields + "]" );
    }

  /**
   * Creates a new TextLine instance. If sourceFields has one field, only the text line will be returned in the
   * subsequent tuples.
   *
   * @param sourceFields the source fields for this scheme
   */
  public TextLine( Fields sourceFields )
    {
    super( sourceFields );

    if( sourceFields.size() < 1 || sourceFields.size() > 2 )
      throw new IllegalArgumentException( "this scheme requires either one or two source fields, given [" + sourceFields + "]" );
    }

  /**
   * Creates a new TextLine instance. If sourceFields has one field, only the text line will be returned in the
   * subsequent tuples. The resulting data set will have numSinkParts.
   *
   * @param sourceFields the source fields for this scheme
   * @param numSinkParts of type int
   */
  public TextLine( Fields sourceFields, int numSinkParts )
    {
    super( sourceFields, numSinkParts );

    if( sourceFields.size() < 1 || sourceFields.size() > 2 )
      throw new IllegalArgumentException( "this scheme requires either one or two source fields, given [" + sourceFields + "]" );
    }

  /**
   * Method getSinkCompression returns the sinkCompression of this TextLine object.
   *
   * @return the sinkCompression (type Compress) of this TextLine object.
   */
  public Compress getSinkCompression()
    {
    return sinkCompression;
    }

  /**
   * Method setSinkCompression sets the sinkCompression of this TextLine object.
   *
   * @param sinkCompression the sinkCompression of this TextLine object.
   */
  public void setSinkCompression( Compress sinkCompression )
    {
    this.sinkCompression = sinkCompression;
    }

  @Override
  public void sourceInit( Tap tap, Job job )
    {
    job.setInputFormatClass( TextInputFormat.class );
    }

  @Override
  public void sinkInit( Tap tap, Job job ) throws IOException
    {
    if( getSinkCompression() == Compress.DISABLE )
      job.getConfiguration().setBoolean( "mapred.output.compress", false );
    else if( getSinkCompression() == Compress.ENABLE )
      job.getConfiguration().setBoolean( "mapred.output.compress", true );

    job.setOutputKeyClass( Text.class ); // be explicit
    job.setOutputValueClass( Text.class ); // be explicit
    job.setOutputFormatClass( TextOutputFormat.class );
    }

  @Override
  public void source( Tuple tuple, TupleEntryCollector tupleEntryCollector )
    {
    Tuple result = new Tuple();

    if( sourceFields.size() == 2 )
      result.add( ( (LongWritable) tuple.get( 0 ) ).get() );

    result.add( tuple.get( 1 ).toString() );

    tupleEntryCollector.add( result );
    }

  @Override
  public void sink( TupleEntry tupleEntry, TupleEntryCollector tupleEntryCollector ) throws IOException
    {
    HadoopFlowProcess hadoopFlowProcess = ( (HadoopEntryCollector) tupleEntryCollector ).getHadoopFlowProcess();
    TaskInputOutputContext taskInputOutputContext = hadoopFlowProcess.getContext();

    try
      {
      // it's ok to use NULL here so the collector does not write anything
      taskInputOutputContext.write( null, tupleEntry.selectTuple( sinkFields ) );
      }
    catch( InterruptedException exception )
      {
      throw new TapException( "thread interrupted", exception );
      }
    }

  }
