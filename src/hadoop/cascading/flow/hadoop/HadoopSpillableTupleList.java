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

package cascading.flow.hadoop;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import cascading.flow.FlowProcess;
import cascading.tuple.SpillableTupleList;
import cascading.tuple.TupleException;
import cascading.tuple.TupleInputStream;
import cascading.tuple.TupleOutputStream;
import cascading.tuple.hadoop.HadoopTupleInputStream;
import cascading.tuple.hadoop.HadoopTupleOutputStream;
import cascading.tuple.hadoop.TupleSerialization;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.JobConf;

/**
 * SpillableTupleList is a simple {@link Iterable} object that can store an unlimited number of {@link cascading.tuple.Tuple} instances by spilling
 * excess to a temporary disk file.
 */
public class HadoopSpillableTupleList extends SpillableTupleList
  {

  /** Field codec */
  private CompressionCodec codec = null;
  /** Field serializationElementWriter */
  private TupleSerialization tupleSerialization;

  /**
   * Constructor SpillableTupleList creates a new SpillableTupleList instance using the given threshold value, and
   * the first available compression codec, if any.
   *
   * @param threshold of type long
   * @param conf
   * @param codec     of type CompressionCodec
   */
  public HadoopSpillableTupleList( long threshold, JobConf conf, CompressionCodec codec )
    {
    this( threshold, conf, codec, null );
    }

  public HadoopSpillableTupleList( long threshold, JobConf conf, CompressionCodec codec, FlowProcess flowProcess )
    {
    super( threshold, flowProcess );
    this.codec = codec;

    if( conf != null )
      tupleSerialization = new TupleSerialization( conf );
    }

  @Override
  protected TupleOutputStream createTupleOutputStream( File file )
    {
    OutputStream outputStream;

    try
      {
      if( codec == null )
        outputStream = new FileOutputStream( file );
      else
        outputStream = codec.createOutputStream( new FileOutputStream( file ) );

      if( tupleSerialization == null )
        return new HadoopTupleOutputStream( outputStream, new TupleSerialization().getElementWriter() );
      else
        return new HadoopTupleOutputStream( outputStream, tupleSerialization.getElementWriter() );
      }
    catch( IOException exception )
      {
      throw new TupleException( "unable to create temporary file input stream", exception );
      }
    }

  @Override
  protected TupleInputStream createTupleInputStream( File file )
    {
    try
      {
      InputStream inputStream;

      if( codec == null )
        inputStream = new FileInputStream( file );
      else
        inputStream = codec.createInputStream( new FileInputStream( file ) );

      if( tupleSerialization == null )
        return new HadoopTupleInputStream( inputStream, new TupleSerialization().getElementReader() );
      else
        return new HadoopTupleInputStream( inputStream, tupleSerialization.getElementReader() );
      }
    catch( IOException exception )
      {
      throw new TupleException( "unable to create temporary file output stream", exception );
      }
    }
  }
