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
import java.io.Serializable;
import java.util.Properties;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.tuple.Fields;

/**
 * Class CompressorScheme a sub-class of {@link Scheme} that provides compression support to any sub-classes.
 * <p>
 * See {@link Compressors} for pre-configured {@link Compressor} implementations.
 */
public abstract class CompressorScheme<SourceContext, SinkContext> extends Scheme<Properties, InputStream, OutputStream, SourceContext, SinkContext>
  {
  public static final Compressor NO_COMPRESSOR = new Compressor()
    {
    @Override
    public InputStream inputStream( InputStream inputStream )
      {
      return inputStream;
      }

    @Override
    public OutputStream outputStream( OutputStream outputStream )
      {
      return outputStream;
      }
    };

  public interface Compressor extends Serializable
    {
    InputStream inputStream( InputStream inputStream ) throws IOException;

    OutputStream outputStream( OutputStream outputStream ) throws IOException;
    }

  protected Compressor compressor = NO_COMPRESSOR;

  public CompressorScheme()
    {
    }

  public CompressorScheme( Fields sourceFields )
    {
    super( sourceFields );
    }

  public CompressorScheme( Fields sourceFields, int numSinkParts )
    {
    super( sourceFields, numSinkParts );
    }

  public CompressorScheme( Fields sourceFields, Fields sinkFields )
    {
    super( sourceFields, sinkFields );
    }

  public CompressorScheme( Fields sourceFields, Fields sinkFields, int numSinkParts )
    {
    super( sourceFields, sinkFields, numSinkParts );
    }

  public CompressorScheme( Compressor compressor )
    {
    if( compressor != null )
      this.compressor = compressor;
    }

  public CompressorScheme( Fields sourceFields, Compressor compressor )
    {
    super( sourceFields );

    if( compressor != null )
      this.compressor = compressor;
    }

  public CompressorScheme( Fields sourceFields, int numSinkParts, Compressor compressor )
    {
    super( sourceFields, numSinkParts );

    if( compressor != null )
      this.compressor = compressor;
    }

  public CompressorScheme( Fields sourceFields, Fields sinkFields, Compressor compressor )
    {
    super( sourceFields, sinkFields );

    if( compressor != null )
      this.compressor = compressor;
    }

  public CompressorScheme( Fields sourceFields, Fields sinkFields, int numSinkParts, Compressor compressor )
    {
    super( sourceFields, sinkFields, numSinkParts );

    if( compressor != null )
      this.compressor = compressor;
    }

  public InputStream sourceWrap( FlowProcess<? extends Properties> flowProcess, InputStream inputStream ) throws IOException
    {
    return compressor.inputStream( inputStream );
    }

  public OutputStream sinkWrap( FlowProcess<? extends Properties> flowProcess, OutputStream outputStream ) throws IOException
    {
    return compressor.outputStream( outputStream );
    }
  }
