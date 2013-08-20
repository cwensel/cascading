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

package cascading.tuple.hadoop.collect;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import cascading.flow.FlowProcess;
import cascading.flow.FlowProcessWrapper;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.tuple.TupleException;
import cascading.tuple.collect.SpillableTupleList;
import cascading.tuple.hadoop.TupleSerialization;
import cascading.tuple.hadoop.io.HadoopTupleInputStream;
import cascading.tuple.hadoop.io.HadoopTupleOutputStream;
import cascading.tuple.io.TupleInputStream;
import cascading.tuple.io.TupleOutputStream;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SpillableTupleList is a simple {@link Iterable} object that can store an unlimited number of {@link cascading.tuple.Tuple} instances by spilling
 * excess to a temporary disk file.
 * <p/>
 * Spills will automatically be compressed using the {@link #defaultCodecs} values. To disable compression or
 * change the codecs, see {@link cascading.tuple.collect.SpillableProps#SPILL_COMPRESS} and {@link cascading.tuple.collect.SpillableProps#SPILL_CODECS}.
 * <p/>
 * It is recommended to add Lzo if available.
 * {@code "org.apache.hadoop.io.compress.LzoCodec,org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec" }
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

      Compressor compressor = null;

      if( codec != null )
        {
        compressor = getCompressor();
        outputStream = codec.createOutputStream( outputStream, compressor );
        }

      final Compressor finalCompressor = compressor;

      return new HadoopTupleOutputStream( outputStream, tupleSerialization.getElementWriter() )
      {
      @Override
      public void close() throws IOException
        {
        try
          {
          super.close();
          }
        finally
          {
          if( finalCompressor != null )
            CodecPool.returnCompressor( finalCompressor );
          }
        }
      };
      }
    catch( IOException exception )
      {
      throw new TupleException( "unable to create temporary file input stream", exception );
      }
    }

  private Compressor getCompressor()
    {
    // some codecs are using direct memory, and the gc for direct memory cannot sometimes keep up
    // so we attempt to force a gc if we see a OOME once.
    try
      {
      return CodecPool.getCompressor( codec );
      }
    catch( OutOfMemoryError error )
      {
      System.gc();
      LOG.info( "received OOME when allocating compressor for codec: {}, retrying once", codec.getClass().getCanonicalName(), error );

      return CodecPool.getCompressor( codec );
      }
    }

  @Override
  protected TupleInputStream createTupleInputStream( File file )
    {
    try
      {
      InputStream inputStream;

      inputStream = new FileInputStream( file );

      Decompressor decompressor = null;

      if( codec != null )
        {
        decompressor = getDecompressor();
        inputStream = codec.createInputStream( inputStream, decompressor );
        }

      final Decompressor finalDecompressor = decompressor;
      return new HadoopTupleInputStream( inputStream, tupleSerialization.getElementReader() )
      {
      @Override
      public void close() throws IOException
        {
        try
          {
          super.close();
          }
        finally
          {
          if( finalDecompressor != null )
            CodecPool.returnDecompressor( finalDecompressor );
          }
        }
      };
      }
    catch( IOException exception )
      {
      throw new TupleException( "unable to create temporary file output stream", exception );
      }
    }

  private Decompressor getDecompressor()
    {
    // some codecs are using direct memory, and the gc for direct memory cannot sometimes keep up
    // so we attempt to force a gc if we see a OOME once.
    try
      {
      return CodecPool.getDecompressor( codec );
      }
    catch( OutOfMemoryError error )
      {
      System.gc();
      LOG.info( "received OOME when allocating decompressor for codec: {}, retrying once", codec.getClass().getCanonicalName(), error );

      return CodecPool.getDecompressor( codec );
      }
    }
  }
