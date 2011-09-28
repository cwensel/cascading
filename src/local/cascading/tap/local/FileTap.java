/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

import cascading.flow.local.LocalFlowProcess;
import cascading.scheme.local.LocalScheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.TupleEntryChainIterator;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleEntrySchemeIterator;
import cascading.util.Util;

/**
 *
 */
public class FileTap extends Tap<LocalFlowProcess, Properties, FileInputStream, FileOutputStream>
  {
  private final String path;

  public FileTap( LocalScheme scheme, String path )
    {
    this( scheme, path, SinkMode.KEEP );
    }

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
  public TupleEntryIterator openForRead( LocalFlowProcess flowProcess, FileInputStream input ) throws IOException
    {
    if( input == null )
      {
      // return an empty iterator
      if( !new File( path ).exists() )
        return new TupleEntryChainIterator( getSourceFields(), new Iterator[ 0 ] );

      input = new FileInputStream( path );
      }

    Closeable reader = (Closeable) ( (LocalScheme) getScheme() ).createInput( input );

    return new TupleEntrySchemeIterator( flowProcess, getScheme(), reader );
    }

  @SuppressWarnings({"ResultOfMethodCallIgnored"})
  @Override
  public TupleEntryCollector openForWrite( LocalFlowProcess flowProcess, FileOutputStream output ) throws IOException
    {
    // ignore the output. will catch the failure downstream if any.
    // not ignoring the output causes race conditions with other systems writing to the same directory.
    new File( path ).getParentFile().mkdirs();

    if( output == null )
      output = new FileOutputStream( path );

    Closeable writer = (Closeable) ( (LocalScheme) getScheme() ).createOutput( output );

    LocalTupleEntryCollector schemeCollector = new LocalTupleEntryCollector( flowProcess, getScheme(), writer );

    schemeCollector.prepare();

    return schemeCollector;
    }

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
