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

package cascading.tap;

import java.beans.ConstructorProperties;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.FlowProcess;
import cascading.scheme.NullScheme;
import cascading.scheme.Scheme;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import org.apache.log4j.Logger;

/**
 * Class MultiSinkTap is both a {@link cascading.tap.CompositeTap} and {@link cascading.tap.SinkTap} that can write to
 * multiple child {@link cascading.tap.Tap} instances simultaneously.
 * <p/>
 * It is the counterpart to {@link cascading.tap.MultiSourceTap}.
 */
public class MultiSinkTap<Process extends FlowProcess<Config>, Config, Input, Output> extends SinkTap<Process, Config, Input, Output> implements CompositeTap
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( MultiSinkTap.class );

  /** Field taps */
  private final Tap[] taps;
  /** Field tempPath */
  private final String tempPath = "__multisink_placeholder" + Integer.toString( (int) ( System.currentTimeMillis() * Math.random() ) );
  /** Field childConfigs */
  private List<Map<String, String>> childConfigs;

  private class MultiSinkCollector extends TupleEntryCollector
    {
    TupleEntryCollector[] collectors;

    public MultiSinkCollector( Process flowProcess, Tap... taps ) throws IOException
      {
      super( Fields.asDeclaration( getSinkFields() ) );

      collectors = new TupleEntryCollector[ taps.length ];

      Config conf = flowProcess.getConfigCopy();

      for( int i = 0; i < taps.length; i++ )
        {
        Config mergedConf = flowProcess.mergeMapIntoConfig( conf, childConfigs.get( i ) );
        Tap tap = taps[ i ];
        LOG.info( "opening for write: " + tap.toString() );

        collectors[ i ] = tap.openForWrite( flowProcess.copyWith( mergedConf ) );
        }
      }

    protected void collect( TupleEntry tupleEntry ) throws IOException
      {
      for( int i = 0; i < taps.length; i++ )
        collectors[ i ].add( tupleEntry );
      }

    @Override
    public void close()
      {
      super.close();

      try
        {
        for( TupleEntryCollector collector : collectors )
          {
          try
            {
            collector.close();
            }
          catch( Exception exception )
            {
            LOG.warn( "exception closing TupleEntryCollector", exception );
            }
          }
        }
      finally
        {
        collectors = null;
        }
      }
    }

  /**
   * Constructor MultiSinkTap creates a new MultiSinkTap instance.
   *
   * @param taps of type Tap...
   */
  @ConstructorProperties({"taps"})
  public MultiSinkTap( Tap... taps )
    {
    this.taps = taps;
    }

  protected Tap[] getTaps()
    {
    return taps;
    }

  @Override
  public Tap[] getChildTaps()
    {
    return Arrays.copyOf( taps, taps.length );
    }

  @Override
  public String getPath()
    {
    return tempPath;
    }

  @Override
  public TupleEntryCollector openForWrite( Process flowProcess, Output output ) throws IOException
    {
    return new MultiSinkCollector( flowProcess, getTaps() );
    }

  @Override
  public void sinkConfInit( Process process, Config conf ) throws IOException
    {
    childConfigs = new ArrayList<Map<String, String>>();

    for( int i = 0; i < getTaps().length; i++ )
      {
      Tap tap = getTaps()[ i ];
      Config jobConf = process.copyConfig( conf );

      tap.sinkConfInit( process, jobConf );

      childConfigs.add( process.diffConfigIntoMap( conf, jobConf ) );
      }
    }

  @Override
  public boolean makeDirs( Config conf ) throws IOException
    {
    for( Tap tap : getTaps() )
      {
      if( !tap.makeDirs( conf ) )
        return false;
      }

    return true;
    }

  @Override
  public boolean deletePath( Config conf ) throws IOException
    {
    for( Tap tap : getTaps() )
      {
      if( !tap.deletePath( conf ) )
        return false;
      }

    return true;
    }

  @Override
  public boolean pathExists( Config conf ) throws IOException
    {
    for( Tap tap : getTaps() )
      {
      if( !tap.pathExists( conf ) )
        return false;
      }

    return true;
    }

  @Override
  public long getPathModified( Config conf ) throws IOException
    {
    long modified = getTaps()[ 0 ].getPathModified( conf );

    for( int i = 1; i < getTaps().length; i++ )
      modified = Math.max( getTaps()[ i ].getPathModified( conf ), modified );

    return modified;
    }

  @Override
  public Scheme getScheme()
    {
    if( super.getScheme() != null )
      return super.getScheme();

    Set<Comparable> fieldNames = new LinkedHashSet<Comparable>();

    for( int i = 0; i < getTaps().length; i++ )
      {
      for( Object o : getTaps()[ i ].getSinkFields() )
        fieldNames.add( (Comparable) o );
      }

    Fields allFields = new Fields( fieldNames.toArray( new Comparable[ fieldNames.size() ] ) );

    setScheme( new NullScheme( allFields, allFields ) );

    return super.getScheme();
    }

  @Override
  public String toString()
    {
    return "MultiSinkTap[" + ( taps == null ? "none" : Arrays.asList( taps ) ) + ']';
    }

  @Override
  public boolean equals( Object o )
    {
    if( this == o )
      return true;
    if( !( o instanceof MultiSinkTap ) )
      return false;
    if( !super.equals( o ) )
      return false;

    MultiSinkTap that = (MultiSinkTap) o;

    if( !Arrays.equals( taps, that.taps ) )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( taps != null ? Arrays.hashCode( taps ) : 0 );
    return result;
    }
  }
