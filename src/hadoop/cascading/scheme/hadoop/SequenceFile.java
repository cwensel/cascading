/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

package cascading.scheme.hadoop;

import java.beans.ConstructorProperties;
import java.io.IOException;

import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.Tuples;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

/**
 * A SequenceFile is a type of {@link cascading.scheme.Scheme}, which is a flat file consisting of
 * binary key/value pairs. This is a space and time efficient means to store data.
 */
public class SequenceFile extends Scheme<HadoopFlowProcess, JobConf, RecordReader, OutputCollector, Object[], Void>
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
  public void sourceConfInit( HadoopFlowProcess flowProcess, Tap tap, JobConf conf )
    {
    conf.setInputFormat( SequenceFileInputFormat.class );
    }

  @Override
  public void sinkConfInit( HadoopFlowProcess flowProcess, Tap tap, JobConf conf )
    {
    conf.setOutputKeyClass( Tuple.class ); // supports TapCollector
    conf.setOutputValueClass( Tuple.class ); // supports TapCollector
    conf.setOutputFormat( SequenceFileOutputFormat.class );
    }

  @Override
  public void sourcePrepare( HadoopFlowProcess flowProcess, SourceCall<Object[], RecordReader> sourceCall )
    {
    Object[] pair = new Object[]{sourceCall.getInput().createKey(), sourceCall.getInput().createValue()};

    sourceCall.setContext( pair );
    }

  @Override
  public boolean source( HadoopFlowProcess flowProcess, SourceCall<Object[], RecordReader> sourceCall ) throws IOException
    {
    Tuple key = (Tuple) sourceCall.getContext()[ 0 ];
    Tuple value = (Tuple) sourceCall.getContext()[ 1 ];
    boolean result = sourceCall.getInput().next( key, value );

    if( !result )
      return false;

    // todo: wrap tuples and defer the addAll
    Tuple tuple = sourceCall.getIncomingEntry().getTuple();
    tuple.clear();

    tuple.addAll( key );
    tuple.addAll( value );

    return true;
    }

  @Override
  public void sourceCleanup( HadoopFlowProcess flowProcess, SourceCall<Object[], RecordReader> sourceCall )
    {
    sourceCall.setContext( null );
    }

  @Override
  public void sink( HadoopFlowProcess flowProcess, SinkCall<Void, OutputCollector> sinkCall ) throws IOException
    {
    sinkCall.getOutput().collect( Tuples.NULL, sinkCall.getOutgoingEntry().getTuple() );
    }
  }
