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

package cascading.tuple;

import java.io.Closeable;
import java.io.File;
import java.io.Flushable;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import cascading.flow.FlowProcess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public abstract class SpillableTupleList implements Iterable<Tuple>
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( SpillableTupleList.class );

  /** Field threshold */
  protected long threshold = 10000;
  /** Field flowProcess */
  protected FlowProcess flowProcess;
  /** Field files */
  private final List<File> files = new LinkedList<File>();
  /** Field current */
  private final List<Tuple> current = new LinkedList<Tuple>();
  /** Field overrideIterator */
  private Iterator<Tuple> overrideIterator;
  /** Field size */
  private long size = 0;
  /** Field fields */
  private Fields fields;

  enum Spill
    {
      Num_Spills_Written, Num_Spills_Read
    }

  protected SpillableTupleList()
    {
    }

  protected SpillableTupleList( long threshold, FlowProcess flowProcess )
    {
    this.threshold = threshold;
    this.flowProcess = flowProcess;
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

  /**
   * Method add will add the given {@link cascading.tuple.Tuple} to this list.
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
   * Method add the given {@link cascading.tuple.TupleEntry} to this list. All TupleEntry instances added must declare the same {@link cascading.tuple.Fields}.
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

    LOG.info( "spilling tuple list to file number {}", getNumFiles() + 1 );

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

  protected abstract TupleOutputStream createTupleOutputStream( File file );

  private List<Tuple> readList( TupleInputStream tupleInputStream )
    {
    try
      {
      long size = tupleInputStream.readLong();
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

  protected abstract TupleInputStream createTupleInputStream( File file );

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
   * Method entryIterator returns a TupleEntry Iterator of all the values in this collection.
   *
   * @return Iterator<TupleEntry>
   */
  public Iterator<TupleEntry> entryIterator()
    {
    return new TupleEntryChainIterator( fields, iterator() );
    }
  }
