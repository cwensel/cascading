/*
 * Copyright (c) 2007-2018 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 */

package cascading.scheme.local;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

import cascading.tap.TapException;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

/**
 * A collection of provided {@link cascading.scheme.local.CompressorScheme.Compressor} implementations backed
 * by <a href="http://commons.apache.org/proper/commons-compress/">Apache Commons Compress</a>.
 */
public class Compressors
  {
  /**
   * The {@link CompressorStreamFactory#BROTLI} compressor.
   */
  public static final ApacheCompressor BROTLI = new ApacheCompressor( CompressorStreamFactory.BROTLI );

  /**
   * The {@link CompressorStreamFactory#BZIP2} compressor.
   */
  public static final ApacheCompressor BZIP2 = new ApacheCompressor( CompressorStreamFactory.BZIP2 );

  /**
   * The {@link CompressorStreamFactory#GZIP} compressor.
   */
  public static final ApacheCompressor GZIP = new ApacheCompressor( CompressorStreamFactory.GZIP );

  /**
   * The {@link CompressorStreamFactory#PACK200} compressor.
   */
  public static final ApacheCompressor PACK200 = new ApacheCompressor( CompressorStreamFactory.PACK200 );

  /**
   * The {@link CompressorStreamFactory#XZ} compressor.
   */
  public static final ApacheCompressor XZ = new ApacheCompressor( CompressorStreamFactory.XZ );

  /**
   * The {@link CompressorStreamFactory#LZMA} compressor.
   */
  public static final ApacheCompressor LZMA = new ApacheCompressor( CompressorStreamFactory.LZMA );

  /**
   * The {@link CompressorStreamFactory#SNAPPY_FRAMED} compressor.
   */
  public static final ApacheCompressor SNAPPY_FRAMED = new ApacheCompressor( CompressorStreamFactory.SNAPPY_FRAMED );

  /**
   * The {@link CompressorStreamFactory#SNAPPY_RAW} compressor.
   */
  public static final ApacheCompressor SNAPPY_RAW = new ApacheCompressor( CompressorStreamFactory.SNAPPY_RAW );

  /**
   * The {@link CompressorStreamFactory#Z} compressor.
   */
  public static final ApacheCompressor Z = new ApacheCompressor( CompressorStreamFactory.Z );

  /**
   * The {@link CompressorStreamFactory#DEFLATE} compressor.
   */
  public static final ApacheCompressor DEFLATE = new ApacheCompressor( CompressorStreamFactory.DEFLATE );

  /**
   * The {@link CompressorStreamFactory#LZ4_BLOCK} compressor.
   */
  public static final ApacheCompressor LZ4_BLOCK = new ApacheCompressor( CompressorStreamFactory.LZ4_BLOCK );

  /**
   * The {@link CompressorStreamFactory#LZ4_FRAMED} compressor.
   */
  public static final ApacheCompressor LZ4_FRAMED = new ApacheCompressor( CompressorStreamFactory.LZ4_FRAMED );

  private static CompressorStreamFactory factory = new CompressorStreamFactory();

  static class ApacheCompressor implements CompressorScheme.Compressor
    {
    String algorithm;

    public ApacheCompressor( String algorithm )
      {
      this.algorithm = algorithm;
      }

    @Override
    public InputStream inputStream( InputStream inputStream ) throws IOException
      {
      try
        {
        return factory.createCompressorInputStream( algorithm, inputStream );
        }
      catch( CompressorException exception )
        {
        throw new TapException( exception );
        }
      }

    @Override
    public OutputStream outputStream( OutputStream outputStream ) throws IOException
      {
      try
        {
        return factory.createCompressorOutputStream( algorithm, outputStream );
        }
      catch( CompressorException exception )
        {
        throw new TapException( exception );
        }
      }

    @Override
    public boolean equals( Object object )
      {
      if( this == object )
        return true;
      if( !( object instanceof ApacheCompressor ) )
        return false;
      ApacheCompressor that = (ApacheCompressor) object;
      return Objects.equals( algorithm, that.algorithm );
      }

    @Override
    public int hashCode()
      {
      return Objects.hash( algorithm );
      }

    @Override
    public String toString()
      {
      final StringBuilder sb = new StringBuilder( "Compressor{" );
      sb.append( "algorithm='" ).append( algorithm ).append( '\'' );
      sb.append( '}' );
      return sb.toString();
      }
    }
  }
