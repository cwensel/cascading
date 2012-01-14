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

package cascading.tuple;

import java.io.Closeable;
import java.io.File;
import java.io.Flushable;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import cascading.flow.FlowProcess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class SpillableTupleList is a simple durable Collection that can spill its contents to disk when the
 * {@code threshold} is met.
 * <p/>
 * Using a {@code threshold } of -1 will disable the spill, all values will remain in memory.
 * <p.></p.>
 * This class is used by the {@link cascading.pipe.CoGroup} pipe, to set properties specific to a given
 * cogroup instance, see the {@link cascading.pipe.CoGroup#getConfigDef()} method.
 *
 * @see cascading.tuple.hadoop.HadoopSpillableTupleList
 */
public abstract class SpillableTupleList implements Collection<Tuple>, Spillable
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( SpillableTupleList.class );

  /** Number of tuples to hold in memory before spilling them to disk. */
  public static final String SPILL_THRESHOLD = "cascading.spill.threshold";

  /**
   * Whether to enable compress of the spills or not, on by default.
   *
   * @see Boolean#parseBoolean(String)
   */
  public static final String SPILL_COMPRESS = "cascading.spill.compress";

  /** A comma delimited list of possible codecs to try. This is platform dependent. */
  public static final String SPILL_CODECS = "cascading.spill.codecs";

  public static final int defaultThreshold = 10 * 1000;

  public static int getThreshold( FlowProcess flowProcess, int defaultValue )
    {
    String value = (String) flowProcess.getProperty( SPILL_THRESHOLD );

    if( value == null || value.length() == 0 )
      return defaultValue;

    return Integer.parseInt( value );
    }

  protected static Class getCodecClass( FlowProcess flowProcess, String defaultCodecs, Class subClass )
    {
    String compress = (String) flowProcess.getProperty( SPILL_COMPRESS );

    if( compress != null && !Boolean.parseBoolean( compress ) )
      return null;

    String codecs = (String) flowProcess.getProperty( SPILL_CODECS );

    if( codecs == null || codecs.length() == 0 )
      codecs = defaultCodecs;

    Class codecClass = null;

    for( String codec : codecs.split( "[,\\s]+" ) )
      {
      try
        {
        LOG.info( "attempting to load codec: {}", codec );
        codecClass = Thread.currentThread().getContextClassLoader().loadClass( codec ).asSubclass( subClass );

        if( codecClass != null )
          {
          LOG.info( "found codec: {}", codec );
          break;
          }
        }
      catch( ClassNotFoundException exception )
        {
        // do nothing
        }
      }

    if( codecClass == null )
      {
      LOG.warn( "codecs set, but unable to load any: {}", codecs );
      return null;
      }

    return codecClass;
    }

  private Threshold threshold;
  /** Field files */
  private final List<File> files = new LinkedList<File>();
  /** Field current */
  private final List<Tuple> current = new LinkedList<Tuple>();
  /** Field overrideIterator */
  private Iterator<Tuple> overrideIterator;
  /** Field size */
  private int size = 0;
  /** Field fields */
  private Fields fields;
  /** Fields listener * */
  private SpillListener spillListener = SpillListener.NULL;

  public interface Threshold
    {
    int current();
    }

  protected SpillableTupleList( final int threshold )
    {
    this( new Threshold()
    {
    @Override
    public int current()
      {
      return threshold;
      }
    } );
    }

  protected SpillableTupleList( Threshold threshold )
    {
    this.threshold = threshold;
    }

  /** Field threshold */
  public long getThreshold()
    {
    return threshold.current();
    }

  @Override
  public void setSpillListener( SpillListener spillListener )
    {
    this.spillListener = spillListener;
    }

  @Override
  public int spillCount()
    {
    return files.size();
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
      spillListener.notifyRead( SpillableTupleList.this );

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

    doSpill();

    return true;
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
  public int size()
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
    if( overrideIterator != null ) // a hack
      return !overrideIterator.hasNext();

    return files.isEmpty() && current.size() == 0;
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
    if( current.size() < getThreshold() ) // may change over time
      return false;

    spillListener.notifySpill( this, current );

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

  public void setOverrideIterator( Iterator<Tuple> overrideIterator )
    {
    this.overrideIterator = overrideIterator;
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
  public TupleEntryIterator entryIterator()
    {
    return new TupleEntryChainIterator( fields, iterator() );
    }

  // collection methods, this class cannot only be added to, so they aren't implemented

  @Override
  public boolean contains( Object object )
    {
    return false;
    }

  @Override
  public Object[] toArray()
    {
    return new Object[ 0 ];
    }

  @Override
  public <T> T[] toArray( T[] ts )
    {
    return null;
    }

  @Override
  public boolean remove( Object object )
    {
    return false;
    }

  @Override
  public boolean containsAll( Collection<?> objects )
    {
    return false;
    }

  @Override
  public boolean addAll( Collection<? extends Tuple> tuples )
    {
    return false;
    }

  @Override
  public boolean removeAll( Collection<?> objects )
    {
    return false;
    }

  @Override
  public boolean retainAll( Collection<?> objects )
    {
    return false;
    }
  }
