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

package cascading.tap;

import java.beans.ConstructorProperties;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.tuple.TupleEntryChainIterator;
import cascading.tuple.TupleEntryIterator;

import static java.util.Arrays.copyOf;

/**
 * Class MultiSourceTap is used to tie multiple {@link cascading.tap.Tap} instances into a single resource. Effectively this will allow
 * multiple files to be concatenated into the requesting pipe assembly, if they all share the same {@link Scheme} instance.
 * <p/>
 * Note that order is not maintained by virtue of the underlying model. If order is necessary, use a unique sequence key
 * to span the resources, like a line number.
 * </p>
 * Note that if multiple input files have the same Scheme (like {@link cascading.scheme.hadoop.TextLine}), they may not contain
 * the same semi-structure internally. For example, one file might be an Apache log file, and another might be a Log4J
 * log file. If each one should be parsed differently, then they must be handled by different pipe assembly branches.
 */
public class MultiSourceTap<Child extends Tap, Config, Input> extends SourceTap<Config, Input> implements CompositeTap<Child>
  {
  private final String identifier = "__multisource_placeholder" + Integer.toString( (int) ( System.currentTimeMillis() * Math.random() ) );
  protected Child[] taps;

  private class TupleIterator implements Iterator
    {
    final TupleEntryIterator iterator;

    private TupleIterator( TupleEntryIterator iterator )
      {
      this.iterator = iterator;
      }

    @Override
    public boolean hasNext()
      {
      return iterator.hasNext();
      }

    @Override
    public Object next()
      {
      return iterator.next().getTuple();
      }

    @Override
    public void remove()
      {
      iterator.remove();
      }
    }

  protected MultiSourceTap( Scheme<Config, Input, ?, ?, ?> scheme )
    {
    super( scheme );
    }

  /**
   * Constructor MultiSourceTap creates a new MultiSourceTap instance.
   *
   * @param taps of type Tap...
   */
  @ConstructorProperties({"taps"})
  public MultiSourceTap( Child... taps )
    {
    this.taps = copyOf( taps, taps.length );

    verifyTaps();
    }

  private void verifyTaps()
    {
    Tap tap = taps[ 0 ];

    for( int i = 1; i < taps.length; i++ )
      {
      if( tap.getClass() != taps[ i ].getClass() )
        throw new TapException( "all taps must be of the same type" );

      if( !tap.getScheme().equals( taps[ i ].getScheme() ) )
        throw new TapException( "all tap schemes must be equivalent" );
      }
    }

  /**
   * Method getTaps returns the taps of this MultiTap object.
   *
   * @return the taps (type Tap[]) of this MultiTap object.
   */
  protected Child[] getTaps()
    {
    return taps;
    }

  @Override
  public Iterator<Child> getChildTaps()
    {
    Child[] taps = getTaps();

    if( taps == null )
      return Collections.EMPTY_LIST.iterator();

    return Arrays.asList( taps ).iterator();
    }

  @Override
  public long getNumChildTaps()
    {
    return getTaps().length;
    }

  @Override
  public String getIdentifier()
    {
    return identifier;
    }

  @Override
  public Scheme getScheme()
    {
    Scheme scheme = super.getScheme();

    if( scheme != null )
      return scheme;

    return taps[ 0 ].getScheme(); // they should all be equivalent per verifyTaps
    }

  @Override
  public boolean isReplace()
    {
    return false; // cannot be used as sink
    }

  @Override
  public void sourceConfInit( FlowProcess<Config> process, Config conf )
    {
    for( Tap tap : getTaps() )
      tap.sourceConfInit( process, conf );
    }

  public boolean resourceExists( Config conf ) throws IOException
    {
    for( Tap tap : getTaps() )
      {
      if( !tap.resourceExists( conf ) )
        return false;
      }

    return true;
    }

  /** Returns the most current modified time. */
  @Override
  public long getModifiedTime( Config conf ) throws IOException
    {
    Tap[] taps = getTaps();

    if( taps == null || taps.length == 0 )
      return 0;

    long modified = taps[ 0 ].getModifiedTime( conf );

    for( int i = 1; i < getTaps().length; i++ )
      modified = Math.max( getTaps()[ i ].getModifiedTime( conf ), modified );

    return modified;
    }

  @Override
  public TupleEntryIterator openForRead( FlowProcess<Config> flowProcess, Input input ) throws IOException
    {
    if( input != null )
      return taps[ 0 ].openForRead( flowProcess, input );

    Iterator iterators[] = new Iterator[ getTaps().length ];

    for( int i = 0; i < getTaps().length; i++ )
      iterators[ i ] = new TupleIterator( getTaps()[ i ].openForRead( flowProcess ) );

    return new TupleEntryChainIterator( getSourceFields(), iterators );
    }

  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( object == null || getClass() != object.getClass() )
      return false;
    if( !super.equals( object ) )
      return false;

    MultiSourceTap multiTap = (MultiSourceTap) object;

    if( !Arrays.equals( getTaps(), multiTap.getTaps() ) )
      return false;

    return true;
    }

  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( getTaps() != null ? Arrays.hashCode( getTaps() ) : 0 );
    return result;
    }

  public String toString()
    {
    Tap[] printableTaps = getTaps();

    if( printableTaps == null )
      return "MultiSourceTap[none]";

    String printedTaps;

    if( printableTaps.length > 10 )
      printedTaps = Arrays.toString( copyOf( printableTaps, 10 ) ) + ",...";
    else
      printedTaps = Arrays.toString( printableTaps );

    return "MultiSourceTap[" + printableTaps.length + ':' + printedTaps + ']';
    }
  }
