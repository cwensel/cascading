/*
 * Copyright (c) 2007-2008 Vinculum Technologies, Inc. All Rights Reserved.
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

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

/**
 * A SequenceFile is a type of {@link Scheme}, which is a flat files consisting of
 * binary key/value pairs.
 */
public class SequenceFile extends Scheme
  {
  /** Field serialVersionUID */
  private static final long serialVersionUID = 1L;
  /** Field NULL */
  private static final Tuple NULL = new Tuple();

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
  public SequenceFile( Fields fields )
    {
    super( fields );
    }

  @Override
  public void sourceInit( JobConf conf )
    {
    conf.setInputFormat( SequenceFileInputFormat.class );
    }

  @Override
  public void sinkInit( JobConf conf )
    {
    conf.setOutputFormat( SequenceFileOutputFormat.class );
    }

  @Override
  public Tuple source( WritableComparable key, Writable value )
    {
    return (Tuple) value;
    }

  @Override
  public InputFormat getInputFormat( JobConf conf )
    {
    return new SequenceFileInputFormat();
    }

  public OutputFormat getOutputFormat( JobConf conf )
    {
    return new SequenceFileOutputFormat();
    }

  @Override
  public void sink( Fields inFields, Tuple tuple, OutputCollector outputCollector ) throws IOException
    {
    Tuple result = sourceFields != null ? tuple.get( inFields, sourceFields ) : tuple;

    outputCollector.collect( NULL, result );
    }

  }
