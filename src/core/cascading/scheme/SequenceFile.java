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

package cascading.scheme;

import java.beans.ConstructorProperties;
import java.io.IOException;

import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.Tuples;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

/**
 * A SequenceFile is a type of {@link Scheme}, which is a flat files consisting of
 * binary key/value pairs. This is a space and time efficient means to store data.
 */
public class SequenceFile extends Scheme
  {
  /** Field serialVersionUID */
  private static final long serialVersionUID = 1L;

  /** Protected for use by TempDfs and other subclasses. Not for general consumption. */
  protected SequenceFile()
    {
    super( null );
    }

  /**
   * Creates a new SequenceFile instance that stores the given field names.
   *
   * @param fields
   */
  @ConstructorProperties({"fields"})
  public SequenceFile( Fields fields )
    {
    super( fields, fields );
    }

  @Override
  public void sourceInit( Tap tap, JobConf conf )
    {
    conf.setInputFormat( SequenceFileInputFormat.class );
    }

  @Override
  public void sinkInit( Tap tap, JobConf conf )
    {
    conf.setOutputKeyClass( Tuple.class ); // supports TapCollector
    conf.setOutputValueClass( Tuple.class ); // supports TapCollector
    conf.setOutputFormat( SequenceFileOutputFormat.class );
    }

  @Override
  public Tuple source( Object key, Object value )
    {
    return (Tuple) value;
    }

  @Override
  public void sink( TupleEntry tupleEntry, OutputCollector outputCollector ) throws IOException
    {
    Tuple result = getSinkFields() != null ? tupleEntry.selectTuple( getSinkFields() ) : tupleEntry.getTuple();

    outputCollector.collect( Tuples.NULL, result );
    }

  }
