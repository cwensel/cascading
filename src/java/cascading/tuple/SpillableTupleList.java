/*
 * Copyright (c) 2007-2008 Vinculum Technologies, Inc. All Rights Reserved.
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

package cascading.tuple;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.Flushable;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * SpillableTupleList is a simple {@link Iterable} object that can store an unlimited number of {@link Tuple} instances by spilling
 * excess to a temporary disk file.
 */
public class SpillableTupleList implements Iterable<Tuple>
  {
  private long threshold = 10000;

  private List<File> files = new LinkedList<File>();
  private List<Tuple> current = new LinkedList<Tuple>();
  private long size = 0;

  public SpillableTupleList( long threshold )
    {
    this.threshold = threshold;
    }

  public void add( Tuple tuple )
    {
    current.add( tuple );
    size++;

    doSpill();
    }

  public long size()
    {
    return size;
    }

  public int getNumFiles()
    {
    return files.size();
    }

  private final void doSpill()
    {
    if( current.size() != threshold )
      return;

    File file = createTempFile();

    DataOutputStream dataOutputStream = createDataOutputStream( file );

    try
      {
      writeList( dataOutputStream, current );
      }
    finally
      {
      flushSilent( dataOutputStream );
      closeSilent( dataOutputStream );
      }

    files.add( file );
    current.clear();
    }

  private void flushSilent( Flushable flushable )
    {
    try
      {
      flushable.flush();
      }
    catch( IOException exception )
      {
      // ignore
      }
    }

  private void closeSilent( Closeable closeable )
    {
    try
      {
      closeable.close();
      }
    catch( IOException exception )
      {
      // ignore
      }
    }

  private void writeList( DataOutputStream dataOutputStream, List<Tuple> list )
    {
    try
      {
      dataOutputStream.writeLong( list.size() );

      for( Tuple tuple : list )
        tuple.write( dataOutputStream );
      }
    catch( IOException exception )
      {
      throw new TupleException( "unable to write to file output stream", exception );
      }
    }

  private DataOutputStream createDataOutputStream( File file )
    {
    try
      {
      return new DataOutputStream( new FileOutputStream( file ) );
      }
    catch( FileNotFoundException exception )
      {
      throw new TupleException( "unable to create temporary file input stream", exception );
      }
    }

  private List<Tuple> readList( DataInputStream dataInputStream )
    {
    try
      {
      long size = dataInputStream.readLong();
      List<Tuple> list = new LinkedList<Tuple>();

      for( int i = 0; i < size; i++ )
        list.add( Tuple.readNewTuple( dataInputStream ) );

      return list;
      }
    catch( IOException exception )
      {
      throw new TupleException( "unable to read from file output stream", exception );
      }
    }

  private DataInputStream createDataInputStream( File file )
    {
    try
      {
      return new DataInputStream( new FileInputStream( file ) );
      }
    catch( FileNotFoundException exception )
      {
      throw new TupleException( "unable to create temporary file output stream", exception );
      }
    }

  private File createTempFile()
    {
    try
      {
      File file = File.createTempFile( "cascading-spillover", null );
      file.deleteOnExit();

      return file;
      }
    catch( IOException exception )
      {
      throw new TupleException( "unable to create temporary file", exception );
      }
    }


  public Iterator<Tuple> iterator()
    {
    if( files.isEmpty() )
      return current.iterator();

    return new SpilledListIterator();
    }

  private class SpilledListIterator implements Iterator<Tuple>
    {
    int fileIndex = 0;
    List<Tuple> currentList;
    private Iterator<Tuple> iterator;

    private SpilledListIterator()
      {
      getNextList();
      }

    private void getNextList()
      {
      if( fileIndex < files.size() )
        currentList = getListFor( files.get( fileIndex++ ) );
      else
        currentList = current;

      iterator = currentList.iterator();
      }

    private List<Tuple> getListFor( File file )
      {
      DataInputStream dataInputStream = createDataInputStream( file );

      try
        {
        return readList( dataInputStream );
        }
      finally
        {
        closeSilent( dataInputStream );
        }
      }

    public boolean hasNext()
      {
      if( currentList == current )
        return iterator.hasNext();

      if( iterator.hasNext() )
        return true;

      getNextList();

      return hasNext();
      }

    public Tuple next()
      {
      if( currentList == current || iterator.hasNext() )
        return iterator.next();

      getNextList();

      return next();
      }

    public void remove()
      {
      throw new UnsupportedOperationException( "remove is not supported" );
      }
    }
  }
