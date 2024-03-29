/*
 * Copyright (c) 2007-2022 The Cascading Authors. All Rights Reserved.
 *
 * Project and contact information: https://cascading.wensel.net/
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

    @Override
    public String getExtension()
      {
      return "";
      }
    };

  public interface Compressor extends Serializable
    {
    InputStream inputStream( InputStream inputStream ) throws IOException;

    OutputStream outputStream( OutputStream outputStream ) throws IOException;

    /**
     * @return null, empty string, or the default extension used for this compression type without a dot.
     */
    String getExtension();
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
    setCompressor( compressor );
    }

  public CompressorScheme( Fields sourceFields, Compressor compressor )
    {
    super( sourceFields );

    setCompressor( compressor );
    }

  public CompressorScheme( Fields sourceFields, int numSinkParts, Compressor compressor )
    {
    super( sourceFields, numSinkParts );

    setCompressor( compressor );
    }

  public CompressorScheme( Fields sourceFields, Fields sinkFields, Compressor compressor )
    {
    super( sourceFields, sinkFields );

    setCompressor( compressor );
    }

  public CompressorScheme( Fields sourceFields, Fields sinkFields, int numSinkParts, Compressor compressor )
    {
    super( sourceFields, sinkFields, numSinkParts );

    setCompressor( compressor );
    }

  protected void setCompressor( Compressor compressor )
    {
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
