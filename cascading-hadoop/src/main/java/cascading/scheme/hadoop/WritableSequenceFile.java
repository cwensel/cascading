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
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

/**
 * Class WritableSequenceFile is a sub-class of {@link SequenceFile} that reads and writes values of the given
 * {@code writableType} {@code Class}, instead of {@link Tuple} instances used by default in SequenceFile.
 * <p/>
 * This Class is a convenience for those who need to read/write specific types from existing sequence files without
 * them being wrapped in a Tuple instance.
 * <p/>
 * Note due to the nature of sequence files, only one type can be stored in the key and value positions, they they can be
 * uniquely different types (LongWritable, Text).
 * <p/>
 * If keyType is null, valueType must not be null, and vice versa, assuming you only wish to store a single value.
 * <p/>
 * {@link NullWritable} is used as the empty type for either a null keyType or valueType.
 */
public class WritableSequenceFile extends SequenceFile
  {
  protected final Class<? extends Writable> keyType;
  protected final Class<? extends Writable> valueType;

  /**
   * Constructor WritableSequenceFile creates a new WritableSequenceFile instance.
   *
   * @param fields    of type Fields
   * @param valueType of type Class<? extends Writable>, may not be null
   */
  @ConstructorProperties({"fields", "valueType"})
  public WritableSequenceFile( Fields fields, Class<? extends Writable> valueType )
    {
    this( fields, null, valueType );
    }

  /**
   * Constructor WritableSequenceFile creates a new WritableSequenceFile instance.
   *
   * @param fields    of type Fields
   * @param keyType   of type Class<? extends Writable>
   * @param valueType of type Class<? extends Writable>
   */
  @ConstructorProperties({"fields", "keyType", "valueType"})
  public WritableSequenceFile( Fields fields, Class<? extends Writable> keyType, Class<? extends Writable> valueType )
    {
    super( fields );
    this.keyType = keyType;
    this.valueType = valueType;

    if( keyType == null && valueType == null )
      throw new IllegalArgumentException( "both keyType and valueType may not be null" );

    if( keyType == null && fields.size() != 1 )
      throw new IllegalArgumentException( "fields must declare exactly one field when only reading/writing 'keys' from a sequence file" );
    else if( valueType == null && fields.size() != 1 )
      throw new IllegalArgumentException( "fields must declare exactly one field when only reading/writing 'values' from a sequence file" );
    else if( keyType != null && valueType != null && fields.size() != 2 )
      throw new IllegalArgumentException( "fields must declare exactly two fields when only reading/writing 'keys' and 'values' from a sequence file" );
    }

  @Override
  public void sinkConfInit( FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf )
    {
    if( keyType != null )
      conf.setOutputKeyClass( keyType );
    else
      conf.setOutputKeyClass( NullWritable.class );

    if( valueType != null )
      conf.setOutputValueClass( valueType );
    else
      conf.setOutputValueClass( NullWritable.class );

    conf.setOutputFormat( SequenceFileOutputFormat.class );
    }

  @Override
  public boolean source( FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall ) throws IOException
    {
    Object key = sourceCall.getContext()[ 0 ];
    Object value = sourceCall.getContext()[ 1 ];
    boolean result = sourceCall.getInput().next( key, value );

    if( !result )
      return false;

    int count = 0;
    TupleEntry entry = sourceCall.getIncomingEntry();

    if( keyType != null )
      entry.setObject( count++, key );

    if( valueType != null )
      entry.setObject( count, value );

    return true;
    }

  @Override
  public void sink( FlowProcess<JobConf> flowProcess, SinkCall<Void, OutputCollector> sinkCall ) throws IOException
    {
    TupleEntry tupleEntry = sinkCall.getOutgoingEntry();

    Writable keyValue = NullWritable.get();
    Writable valueValue = NullWritable.get();

    if( keyType == null )
      {
      valueValue = (Writable) tupleEntry.getObject( 0 );
      }
    else if( valueType == null )
      {
      keyValue = (Writable) tupleEntry.getObject( 0 );
      }
    else
      {
      keyValue = (Writable) tupleEntry.getObject( 0 );
      valueValue = (Writable) tupleEntry.getObject( 1 );
      }

    sinkCall.getOutput().collect( keyValue, valueValue );
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof WritableSequenceFile ) )
      return false;
    if( !super.equals( object ) )
      return false;

    WritableSequenceFile that = (WritableSequenceFile) object;

    if( keyType != null ? !keyType.equals( that.keyType ) : that.keyType != null )
      return false;
    if( valueType != null ? !valueType.equals( that.valueType ) : that.valueType != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( keyType != null ? keyType.hashCode() : 0 );
    result = 31 * result + ( valueType != null ? valueType.hashCode() : 0 );
    return result;
    }
  }
