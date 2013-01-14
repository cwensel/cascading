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

package cascading.scheme.hadoop;

import java.beans.ConstructorProperties;
import java.io.IOException;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

/**
 * A SequenceFile is a type of {@link cascading.scheme.Scheme}, which is a flat file consisting of
 * binary key/value pairs. This is a space and time efficient means to store data.
 */
public class SequenceFile extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Void>
  {
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
  public void sourceConfInit( FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf )
    {
    conf.setInputFormat( SequenceFileInputFormat.class );
    }

  @Override
  public void sinkConfInit( FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf )
    {
    conf.setOutputKeyClass( Tuple.class ); // supports TapCollector
    conf.setOutputValueClass( Tuple.class ); // supports TapCollector
    conf.setOutputFormat( SequenceFileOutputFormat.class );
    }

  @Override
  public void sourcePrepare( FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall )
    {
    Object[] pair = new Object[]{
      sourceCall.getInput().createKey(),
      sourceCall.getInput().createValue()
    };

    sourceCall.setContext( pair );
    }

  @Override
  public boolean source( FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall ) throws IOException
    {
    Tuple key = (Tuple) sourceCall.getContext()[ 0 ];
    Tuple value = (Tuple) sourceCall.getContext()[ 1 ];
    boolean result = sourceCall.getInput().next( key, value );

    if( !result )
      return false;

    TupleEntry entry = sourceCall.getIncomingEntry();

    if( entry.hasTypes() )
      entry.setCanonicalTuple( value );
    else
      entry.setTuple( value );

    return true;
    }

  @Override
  public void sourceCleanup( FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall )
    {
    sourceCall.setContext( null );
    }

  @Override
  public void sink( FlowProcess<JobConf> flowProcess, SinkCall<Void, OutputCollector> sinkCall ) throws IOException
    {
    sinkCall.getOutput().collect( Tuple.NULL, sinkCall.getOutgoingEntry().getTuple() );
    }
  }
