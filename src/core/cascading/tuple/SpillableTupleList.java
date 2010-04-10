/*
 * Copyright (c) 2007-2010 Concurrent, Inc. All Rights Reserved.
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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.Flushable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import cascading.flow.FlowProcess;
import cascading.tuple.hadoop.TupleSerialization;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

/**
 * SpillableTupleList is a simple {@link Iterable} object that can store an unlimited number of {@link Tuple} instances by spilling
 * excess to a temporary disk file.
 */
public class SpillableTupleList implements Iterable<Tuple>
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( SpillableTupleList.class );

  enum Spill
    {
      Num_Spills_Written, Num_Spills_Read
    }

  /** Field threshold */
  private long threshold = 10000;
  /** Field codec */
  private CompressionCodec codec = null;
  /** Field flowProcess */
  private FlowProcess flowProcess;
  /** Field files */
  private List<File> files = new LinkedList<File>();
  /** Field current */
  private List<Tuple> current = new LinkedList<Tuple>();
  /** Field overrideIterator */
  private Iterator<Tuple> overrideIterator;
  /** Field size */
  private long size = 0;
  /** Field fields */
  private Fields fields;
  /** Field serializationElementWriter */
  private TupleSerialization tupleSerialization;

  /** Constructor SpillableTupleList creates a new SpillableTupleList instance. */
  public SpillableTupleList()
    {
    }

  /**
   * Constructor SpillableTupleList creates a new SpillableTupleList instance using the given threshold value.
   *
   * @param threshold of type long
   */
  public SpillableTupleList( long threshold )
    {
    this.threshold = threshold;
    }

  /**
   * Constructor SpillableTupleList creates a new SpillableTupleList instance using the given threshold value, and
   * the first available compression codec, if any.
   *
   * @param threshold of type long
   * @param conf
   * @param codec     of type CompressionCodec
   */
  public SpillableTupleList( long threshold, JobConf conf, CompressionCodec codec )
    {
    this( threshold, conf, codec, null );
    }

  public SpillableTupleList( long threshold, JobConf conf, CompressionCodec codec, FlowProcess flowProcess )
    {
    this.threshold = threshold;
    this.codec = codec;
    this.flowProcess = flowProcess;

    if( conf != null )
      tupleSerialization = new TupleSerialization( conf );
    }

  /**
   * Method add will add the given {@link Tuple} to this list.
   *
   * @param tuple of type Tuple
   */
  public boolean add( Tuple tuple )
    {
    current.add( tuple );
    size++;

    return doSpill();
    }

  /**
   * Method add the given {@link TupleEntry} to this list. All TupleEntry instances added must declare the same {@link Fields}.
   *
   * @param tupleEntry of type TupleEntry
   */
  public boolean add( TupleEntry tupleEntry )
    {
    if( fields == null )
      fields = tupleEntry.fields;
    else if( !fields.equals( tupleEntry.fields ) )
      throw new IllegalArgumentException( "all entries must have same fields, have: " + fields.print() + " got: " + tupleEntry.fields.print() );

    return add( tupleEntry.getTuple() );
    }

  /**
   * Method size returns the size of this list.
   *
   * @return long
   */
  public long size()
    {
    return size;
    }

  /**
   * Method isEmpty returns true if this list is empty
   *
   * @return the empty (type boolean) of this SpillableTupleList object.
   */
  public boolean isEmpty()
    {
    return overrideIterator == null && files.isEmpty() && current.size() == 0;
    }

  /**
   * Method getNumFiles returns the number of files this list has spilled to.
   *
   * @return the numFiles (type int) of this SpillableTupleList object.
   */
  public int getNumFiles()
    {
    return files.size();
    }

  private final boolean doSpill()
    {
    if( current.size() != threshold )
      return false;

    LOG.info( "spilling tuple list to file number " + ( getNumFiles() + 1 ) );

    if( flowProcess != null )
      flowProcess.increment( Spill.Num_Spills_Written, 1 );

    File file = createTempFile();
    TupleOutputStream dataOutputStream = createTupleOutputStream( file );

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

    return true;
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

  private void writeList( TupleOutputStream dataOutputStream, List<Tuple> list )
    {
    try
      {
      dataOutputStream.writeLong( list.size() );

      for( Tuple tuple : list )
        dataOutputStream.writeTuple( tuple );
      }
    catch( IOException exception )
      {
      throw new TupleException( "unable to write to file output stream", exception );
      }
    }

  private TupleOutputStream createTupleOutputStream( File file )
    {
    OutputStream outputStream;

    try
      {
      if( codec == null )
        outputStream = new FileOutputStream( file );
      else
        outputStream = codec.createOutputStream( new FileOutputStream( file ) );

      if( tupleSerialization == null )
        return new TupleOutputStream( outputStream );
      else
        return new TupleOutputStream( outputStream, tupleSerialization.getElementWriter() );
      }
    catch( IOException exception )
      {
      throw new TupleException( "unable to create temporary file input stream", exception );
      }
    }

  private List<Tuple> readList( TupleInputStream tupleInputStream )
    {
    try
      {
      long size = tupleInputStream.readVLong();
      List<Tuple> list = new LinkedList<Tuple>();

      for( int i = 0; i < size; i++ )
        list.add( tupleInputStream.readTuple() );

      return list;
      }
    catch( IOException exception )
      {
      throw new TupleException( "unable to read from file output stream", exception );
      }
    }

  private TupleInputStream createTupleInputStream( File file )
    {
    try
      {
      InputStream inputStream;

      if( codec == null )
        inputStream = new FileInputStream( file );
      else
        inputStream = codec.createInputStream( new FileInputStream( file ) );

      if( tupleSerialization == null )
        return new TupleInputStream( inputStream );
      else
        return new TupleInputStream( inputStream, tupleSerialization.getElementReader() );
      }
    catch( IOException exception )
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

  /** Method clear empties this container so it may be re-used. */
  public void clear()
    {
    overrideIterator = null;
    files.clear();
    current.clear();
    size = 0;
    }

  public void setIterator( final IndexTuple current, final Iterator values )
    {
    overrideIterator = new Iterator<Tuple>()
    {
    IndexTuple value = current;

    @Override
    public boolean hasNext()
      {
      return value != null;
      }

    @Override
    public Tuple next()
      {
      Tuple result = value.getTuple();

      if( values.hasNext() )
        value = (IndexTuple) values.next();
      else
        value = null;

      return result;
      }

    @Override
    public void remove()
      {
      // unsupported
      }
    };
    }

  /**
   * Method iterator returns a Tuple Iterator of all the values in this collection.
   *
   * @return Iterator<Tuple>
   */
  public Iterator<Tuple> iterator()
    {
    if( overrideIterator != null )
      return overrideIterator;

    if( files.isEmpty() )
      return current.iterator();

    return new SpilledListIterator();
    }

  /**
   * Method entryIterator returns a TupleEntry Iterator of all the alues in this collection.
   *
   * @return Iterator<TupleEntry>
   */
  public Iterator<TupleEntry> entryIterator()
    {
    return new TupleEntryIterator( fields, iterator() );
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
      if( flowProcess != null )
        flowProcess.increment( Spill.Num_Spills_Read, 1 );

      TupleInputStream dataInputStream = createTupleInputStream( file );

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
