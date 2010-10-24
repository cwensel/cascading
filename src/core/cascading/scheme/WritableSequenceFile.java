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

import java.io.IOException;

import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;

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
  protected Class<? extends Writable> keyType;
  protected Class<? extends Writable> valueType;

  /**
   * Constructor WritableSequenceFile creates a new WritableSequenceFile instance.
   *
   * @param fields    of type Fields
   * @param valueType of type Class<? extends Writable>, may not be null
   */
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
  public void sinkInit( Tap tap, JobConf conf )
    {
    super.sinkInit( tap, conf );

    if( keyType != null )
      conf.setOutputKeyClass( keyType );
    else
      conf.setOutputKeyClass( NullWritable.class );

    if( valueType != null )
      conf.setOutputValueClass( valueType );
    else
      conf.setOutputValueClass( NullWritable.class );
    }

  @Override
  public Tuple source( Object key, Object value )
    {
    if( keyType == null )
      return new Tuple( value );

    if( valueType == null )
      return new Tuple( key );

    return new Tuple( key, value );
    }

  @Override
  public void sink( TupleEntry tupleEntry, OutputCollector outputCollector ) throws IOException
    {
    Object keyValue = NullWritable.get();
    Object valueValue = NullWritable.get();

    if( keyType == null )
      {
      valueValue = tupleEntry.getObject( getSinkFields() );
      }
    else if( valueType == null )
      {
      keyValue = tupleEntry.getObject( getSinkFields() );
      }
    else
      {
      keyValue = tupleEntry.getObject( getSinkFields().get( 0 ) );
      valueValue = tupleEntry.getObject( getSinkFields().get( 1 ) );
      }

    outputCollector.collect( keyValue, valueValue );
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
