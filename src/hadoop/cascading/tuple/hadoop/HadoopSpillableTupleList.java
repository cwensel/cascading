/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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

package cascading.tuple.hadoop;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import cascading.flow.FlowProcess;
import cascading.flow.FlowProcessWrapper;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.tuple.SpillableTupleList;
import cascading.tuple.TupleException;
import cascading.tuple.TupleInputStream;
import cascading.tuple.TupleOutputStream;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SpillableTupleList is a simple {@link Iterable} object that can store an unlimited number of {@link cascading.tuple.Tuple} instances by spilling
 * excess to a temporary disk file.
 * <p/>
 * Spills will automatically be compressed using the {@link #defaultCodecs} values. To disable compression or
 * change the codecs, see {@link SpillableTupleList#SPILL_COMPRESS} and {@link SpillableTupleList#SPILL_CODECS}.
 */
public class HadoopSpillableTupleList extends SpillableTupleList
  {
  private static final Logger LOG = LoggerFactory.getLogger( HadoopSpillableTupleList.class );

  public static final String defaultCodecs = "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec";

  /** Field codec */
  private final CompressionCodec codec;
  /** Field serializationElementWriter */
  private final TupleSerialization tupleSerialization;

  public static synchronized CompressionCodec getCodec( FlowProcess flowProcess, String defaultCodecs )
    {
    Class<? extends CompressionCodec> codecClass = getCodecClass( flowProcess, defaultCodecs, CompressionCodec.class );

    if( codecClass == null )
      return null;

    if( flowProcess instanceof FlowProcessWrapper )
      flowProcess = ( (FlowProcessWrapper) flowProcess ).getDelegate();

    return ReflectionUtils.newInstance( codecClass, ( (HadoopFlowProcess) flowProcess ).getJobConf() );
    }

  /**
   * Constructor SpillableTupleList creates a new SpillableTupleList instance using the given threshold value, and
   * the first available compression codec, if any.
   *
   * @param threshold of type long
   * @param codec     of type CompressionCodec
   */
  public HadoopSpillableTupleList( int threshold, CompressionCodec codec, JobConf jobConf )
    {
    super( threshold );
    this.codec = codec;

    if( jobConf == null )
      this.tupleSerialization = new TupleSerialization();
    else
      this.tupleSerialization = new TupleSerialization( jobConf );
    }

  public HadoopSpillableTupleList( int threshold, TupleSerialization tupleSerialization, CompressionCodec codec )
    {
    super( threshold );
    this.tupleSerialization = tupleSerialization;
    this.codec = codec;
    }

  @Override
  protected TupleOutputStream createTupleOutputStream( File file )
    {
    OutputStream outputStream;

    try
      {
      outputStream = new FileOutputStream( file );

      if( codec != null )
        outputStream = createCodecOutputStream( outputStream );

      return new HadoopTupleOutputStream( outputStream, tupleSerialization.getElementWriter() );
      }
    catch( IOException exception )
      {
      throw new TupleException( "unable to create temporary file input stream", exception );
      }
    }

  private CompressionOutputStream createCodecOutputStream( OutputStream outputStream ) throws IOException
    {
    // some codecs are using direct memory, and the gc for direct memory cannot sometimes keep up
    // so we attempt to force a gc if we see a OOME once.
    try
      {
      return codec.createOutputStream( outputStream );
      }
    catch( OutOfMemoryError error )
      {
      LOG.info( "received OOME when allocating output stream for codec: {}, retrying once", codec.getClass().getCanonicalName(), error );
      System.gc();

      return codec.createOutputStream( outputStream );
      }
    }

  @Override
  protected TupleInputStream createTupleInputStream( File file )
    {
    try
      {
      InputStream inputStream;

      inputStream = new FileInputStream( file );

      if( codec != null )
        inputStream = createCodecInputStream( inputStream );

      return new HadoopTupleInputStream( inputStream, tupleSerialization.getElementReader() );
      }
    catch( IOException exception )
      {
      throw new TupleException( "unable to create temporary file output stream", exception );
      }
    }

  private CompressionInputStream createCodecInputStream( InputStream inputStream ) throws IOException
    {
    // some codecs are using direct memory, and the gc for direct memory cannot sometimes keep up
    // so we attempt to force a gc if we see a OOME once.
    try
      {
      return codec.createInputStream( inputStream );
      }
    catch( OutOfMemoryError error )
      {
      LOG.info( "received OOME when allocating input stream for codec: {}, retrying once", codec.getClass().getCanonicalName(), error );
      System.gc();

      return codec.createInputStream( inputStream );
      }
    }
  }
