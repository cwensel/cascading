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

package cascading.tuple.collect;

import java.io.Closeable;
import java.io.File;
import java.io.Flushable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import cascading.flow.FlowProcess;
import cascading.tuple.Tuple;
import cascading.tuple.TupleException;
import cascading.tuple.io.TupleInputStream;
import cascading.tuple.io.TupleOutputStream;
import cascading.tuple.util.TupleViews;
import cascading.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class SpillableTupleList is a simple durable Collection that can spill its contents to disk when the
 * {@code threshold} is met.
 * <p/>
 * Using a {@code threshold } of -1 will disable the spill, all values will remain in memory.
 * <p.></p.>
 * This class is used by the {@link cascading.pipe.CoGroup} pipe, to set properties specific to a given
 * CoGroup instance, see the {@link cascading.pipe.CoGroup#getConfigDef()} method.
 * <p/>
 * Use the {@link SpillableProps} fluent helper class to set properties.
 *
 * @see cascading.tuple.hadoop.collect.HadoopSpillableTupleList
 */
public abstract class SpillableTupleList implements Collection<Tuple>, Spillable
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( SpillableTupleList.class );

  /** Number of tuples to hold in memory before spilling them to disk. */
  @Deprecated
  public static final String SPILL_THRESHOLD = SpillableProps.LIST_THRESHOLD;

  /**
   * Whether to enable compress of the spills or not, on by default.
   *
   * @see Boolean#parseBoolean(String)
   */
  @Deprecated
  public static final String SPILL_COMPRESS = SpillableProps.SPILL_COMPRESS;

  /** A comma delimited list of possible codecs to try. This is platform dependent. */
  @Deprecated
  public static final String SPILL_CODECS = SpillableProps.SPILL_CODECS;

  public static int getThreshold( FlowProcess flowProcess, int defaultValue )
    {
    String value = (String) flowProcess.getProperty( SpillableProps.LIST_THRESHOLD );

    if( value == null || value.length() == 0 )
      return defaultValue;

    return Integer.parseInt( value );
    }

  protected static Class getCodecClass( FlowProcess flowProcess, String defaultCodecs, Class subClass )
    {
    String compress = (String) flowProcess.getProperty( SpillableProps.SPILL_COMPRESS );

    if( compress != null && !Boolean.parseBoolean( compress ) )
      return null;

    String codecs = (String) flowProcess.getProperty( SpillableProps.SPILL_CODECS );

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


  private SpillStrategy spillStrategy;

  /** Field files */
  private List<File> files = Collections.EMPTY_LIST; // lazy init if we do a spill
  /** Field current */
  private final List<Object[]> current = new LinkedList<Object[]>();
  /** Field size */
  private int size = 0;
  /** Fields listener */
  private SpillListener spillListener = SpillListener.NULL;

  private Tuple group;

  protected SpillableTupleList( final int threshold )
    {
    this( new SpillStrategy()
    {

    @Override
    public boolean doSpill( Spillable spillable, int size )
      {
      return size >= threshold;
      }

    @Override
    public String getSpillReason( Spillable spillable )
      {
      return "met threshold: " + threshold;
      }
    } );
    }

  protected SpillableTupleList( SpillStrategy spillStrategy )
    {
    this.spillStrategy = spillStrategy;
    }

  @Override
  public void setGrouping( Tuple group )
    {
    this.group = group;
    }

  @Override
  public Tuple getGrouping()
    {
    return group;
    }

  @Override
  public void setSpillStrategy( SpillStrategy spillStrategy )
    {
    this.spillStrategy = spillStrategy;
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
    private Iterator<Tuple> lastIterator;
    private Iterator<Tuple> iterator;

    private SpilledListIterator()
      {
      lastIterator = asTupleIterator();
      getNextIterator();
      }

    private void getNextIterator()
      {
      if( iterator instanceof Closeable )
        closeSilent( (Closeable) iterator );

      if( fileIndex < files.size() )
        iterator = getIteratorFor( files.get( fileIndex++ ) );
      else
        iterator = lastIterator;
      }

    private Iterator<Tuple> getIteratorFor( File file )
      {
      spillListener.notifyReadSpillBegin( SpillableTupleList.this );

      return createIterator( createTupleInputStream( file ) );
      }

    public boolean hasNext()
      {
      if( isLastCollection() )
        return iterator.hasNext();

      if( iterator.hasNext() )
        return true;

      getNextIterator();

      return hasNext();
      }

    public Tuple next()
      {
      if( isLastCollection() || iterator.hasNext() )
        return iterator.next();

      getNextIterator();

      return next();
      }

    private boolean isLastCollection()
      {
      return iterator == lastIterator;
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
  @Override
  public boolean add( Tuple tuple )
    {
    doSpill(); // spill if we break over the threshold

    current.add( Tuple.elements( tuple ).toArray( new Object[ tuple.size() ] ) );
    size++;

    return true;
    }

  @Override
  public int size()
    {
    return size;
    }

  @Override
  public boolean isEmpty()
    {
    return files.isEmpty() && current.size() == 0;
    }

  private final boolean doSpill()
    {
    if( !spillStrategy.doSpill( this, current.size() ) )
      return false;

    long start = System.currentTimeMillis();
    spillListener.notifyWriteSpillBegin( this, current.size(), spillStrategy.getSpillReason( this ) );

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

    spillListener.notifyWriteSpillEnd( this, System.currentTimeMillis() - start );

    if( files == Collections.EMPTY_LIST )
      files = new LinkedList<File>();

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

  private void writeList( TupleOutputStream dataOutputStream, List<Object[]> list )
    {
    try
      {
      dataOutputStream.writeLong( list.size() );

      for( Object[] elements : list )
        dataOutputStream.writeElementArray( elements );
      }
    catch( IOException exception )
      {
      throw new TupleException( "unable to write tuple collection to file output stream", exception );
      }
    }

  protected abstract TupleOutputStream createTupleOutputStream( File file );

  private Iterator<Tuple> createIterator( final TupleInputStream tupleInputStream )
    {
    final long size;

    try
      {
      size = tupleInputStream.readLong();
      }
    catch( IOException exception )
      {
      throw new TupleException( "unable to read 'size' of collection from file input stream", exception );
      }

    return new CloseableIterator<Tuple>()
    {
    Tuple tuple = new Tuple();
    long count = 0;

    @Override
    public boolean hasNext()
      {
      return count < size;
      }

    @Override
    public Tuple next()
      {
      try
        {
        return tupleInputStream.readTuple( tuple );
        }
      catch( IOException exception )
        {
        throw new TupleException( "unable to read next tuple from file input stream containing: " + size + " tuples, successfully read tuples: " + count, exception );
        }
      finally
        {
        count++;
        }
      }

    @Override
    public void remove()
      {
      throw new UnsupportedOperationException( "remove is not supported" );
      }

    @Override
    public void close() throws IOException
      {
      tupleInputStream.close();
      }
    };
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

  @Override
  public void clear()
    {
    files.clear();
    current.clear();
    size = 0;
    }

  @Override
  public Iterator<Tuple> iterator()
    {
    if( files.isEmpty() )
      return asTupleIterator();

    return new SpilledListIterator();
    }

  private Iterator<Tuple> asTupleIterator()
    {
    final Tuple tuple = TupleViews.createObjectArray();
    final Iterator<Object[]> iterator = current.iterator();

    return new Iterator<Tuple>()
    {
    @Override
    public boolean hasNext()
      {
      return iterator.hasNext();
      }

    @Override
    public Tuple next()
      {
      return TupleViews.reset( tuple, iterator.next() );
      }

    @Override
    public void remove()
      {
      }
    };
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
