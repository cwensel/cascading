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

package cascading.tap.local;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import cascading.flow.FlowProcess;
import cascading.scheme.local.LocalScheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import cascading.util.Util;

/**
 * Class FileTap is a {@link Tap} sub-class that allows for direct local file access.
 * <p/>
 * FileTap must be used with the {@link cascading.flow.local.LocalFlowConnector} to create
 * {@link cascading.flow.Flow} instances that run in "local" mode.
 */
public class FileTap extends Tap<FlowProcess<Properties>, Properties, FileInputStream, FileOutputStream>
  {
  private final String path;

  /**
   * Constructor FileTap creates a new FileTap instance using the given {@link cascading.scheme.Scheme} and file {@code path}.
   *
   * @param scheme of type LocalScheme
   * @param path   of type String
   */
  public FileTap( LocalScheme scheme, String path )
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
  public FileTap( LocalScheme scheme, String path, SinkMode sinkMode )
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
  public TupleEntryIterator openForRead( FlowProcess<Properties> flowProcess, FileInputStream input ) throws IOException
    {
    if( input == null )
      input = new FileInputStream( path );

    return new LocalTupleEntrySchemeIterator( flowProcess, (LocalScheme) getScheme(), input, path );
    }

  @Override
  public TupleEntryCollector openForWrite( FlowProcess<Properties> flowProcess, FileOutputStream output ) throws IOException
    {
    // ignore the output. will catch the failure downstream if any.
    // not ignoring the output causes race conditions with other systems writing to the same directory.
    File parentFile = new File( path ).getAbsoluteFile().getParentFile();

    if( parentFile != null && parentFile.exists() && parentFile.isFile() )
      throw new TapException( "cannot create parent directory, it already exists as a file: " + parentFile.getAbsolutePath() );

    // don't test for success, just fighting a race condition otherwise
    // will get caught downstream
    if( parentFile != null )
      parentFile.mkdirs();

    if( output == null )
      output = new FileOutputStream( path, isUpdate() ); // append if we are in update mode

    return new LocalTupleEntryCollector( flowProcess, (LocalScheme) getScheme(), output, path );
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
    File file = new File( path );

    if( file.isDirectory() )
      return 0;

    return file.length();
    }

  @Override
  public boolean createResource( Properties conf ) throws IOException
    {
    File parentFile = new File( path ).getParentFile();

    return parentFile.exists() || parentFile.mkdirs();
    }

  @Override
  public boolean deleteResource( Properties conf ) throws IOException
    {
    return new File( path ).delete();
    }

  @Override
  public boolean commitResource( Properties conf ) throws IOException
    {
    return true;
    }

  @Override
  public boolean resourceExists( Properties conf ) throws IOException
    {
    return new File( path ).exists();
    }

  @Override
  public long getModifiedTime( Properties conf ) throws IOException
    {
    return new File( path ).lastModified();
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof FileTap ) )
      return false;
    if( !super.equals( object ) )
      return false;

    FileTap fileTap = (FileTap) object;

    if( path != null ? !path.equals( fileTap.path ) : fileTap.path != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( path != null ? path.hashCode() : 0 );
    return result;
    }

  /** @see Object#toString() */
  @Override
  public String toString()
    {
    if( path != null )
      return getClass().getSimpleName() + "[\"" + getScheme() + "\"]" + "[\"" + Util.sanitizeUrl( path ) + "\"]"; // sanitize
    else
      return getClass().getSimpleName() + "[\"" + getScheme() + "\"]" + "[not initialized]";
    }
  }
