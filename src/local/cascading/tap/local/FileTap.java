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
import cascading.tap.TapException;
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
  public String getPath()
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

  @Override
  public TupleEntryCollector openForWrite( LocalFlowProcess flowProcess, FileOutputStream output ) throws IOException
    {
    File parentFile = new File( path ).getParentFile();

    if( !parentFile.exists() && !parentFile.mkdirs() )
      throw new TapException( "unable to mkdirs for: " + path );

    if( output == null )
      output = new FileOutputStream( path );

    Closeable writer = (Closeable) ( (LocalScheme) getScheme() ).createOutput( output );

    return new LocalTupleEntryCollector( flowProcess, getScheme(), writer );
    }

  @Override
  public boolean makeDirs( Properties conf ) throws IOException
    {
    return new File( path ).getParentFile().mkdirs();
    }

  @Override
  public boolean deletePath( Properties conf ) throws IOException
    {
    return new File( path ).delete();
    }

  @Override
  public boolean pathExists( Properties conf ) throws IOException
    {
    return new File( path ).exists();
    }

  @Override
  public long getPathModified( Properties conf ) throws IOException
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
