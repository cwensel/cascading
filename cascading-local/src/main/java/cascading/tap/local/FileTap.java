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

package cascading.tap.local;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.io.TapFileOutputStream;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleEntrySchemeCollector;
import cascading.tuple.TupleEntrySchemeIterator;

/**
 * Class FileTap is a {@link Tap} sub-class that allows for direct local file access.
 * <p/>
 * FileTap must be used with the {@link cascading.flow.local.LocalFlowConnector} to create
 * {@link cascading.flow.Flow} instances that run in "local" mode.
 */
public class FileTap extends Tap<Properties, InputStream, OutputStream>
  {
  private final String path;

  /**
   * Constructor FileTap creates a new FileTap instance using the given {@link cascading.scheme.Scheme} and file {@code path}.
   *
   * @param scheme of type LocalScheme
   * @param path   of type String
   */
  public FileTap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, String path )
    {
    this( scheme, path, SinkMode.KEEP );
    }

  /**
   * Constructor FileTap creates a new FileTap instance using the given {@link cascading.scheme.Scheme},
   * file {@code path}, and {@code SinkMode}.
   *
   * @param scheme   of type LocalScheme
   * @param path     of type String
   * @param sinkMode of type SinkMode
   */
  public FileTap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, String path, SinkMode sinkMode )
    {
    super( scheme, sinkMode );
    this.path = path;
    }

  @Override
  public String getIdentifier()
    {
    return path;
    }

  @Override
  public TupleEntryIterator openForRead( FlowProcess<Properties> flowProcess, InputStream input ) throws IOException
    {
    if( input == null )
      input = new FileInputStream( getIdentifier() );

    return new TupleEntrySchemeIterator<Properties, InputStream>( flowProcess, getScheme(), input, getIdentifier() );
    }

  @Override
  public TupleEntryCollector openForWrite( FlowProcess<Properties> flowProcess, OutputStream output ) throws IOException
    {
    if( output == null )
      output = new TapFileOutputStream( getIdentifier(), isUpdate() ); // append if we are in update mode

    return new TupleEntrySchemeCollector<Properties, OutputStream>( flowProcess, getScheme(), output, getIdentifier() );
    }

  /**
   * Method getSize returns the size of the file referenced by this tap.
   *
   * @param conf of type Properties
   * @return The size of the file reference by this tap.
   * @throws IOException
   */
  public long getSize( Properties conf ) throws IOException
    {
    File file = new File( getIdentifier() );

    if( file.isDirectory() )
      return 0;

    return file.length();
    }

  @Override
  public boolean createResource( Properties conf ) throws IOException
    {
    File parentFile = new File( getIdentifier() ).getParentFile();

    return parentFile.exists() || parentFile.mkdirs();
    }

  @Override
  public boolean deleteResource( Properties conf ) throws IOException
    {
    return new File( getIdentifier() ).delete();
    }

  @Override
  public boolean commitResource( Properties conf ) throws IOException
    {
    return true;
    }

  @Override
  public boolean resourceExists( Properties conf ) throws IOException
    {
    return new File( getIdentifier() ).exists();
    }

  @Override
  public long getModifiedTime( Properties conf ) throws IOException
    {
    return new File( getIdentifier() ).lastModified();
    }
  }
