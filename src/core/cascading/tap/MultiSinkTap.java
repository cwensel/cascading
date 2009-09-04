/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.FlowContext;
import cascading.flow.hadoop.ConfFlowContext;
import cascading.scheme.Scheme;
import cascading.scheme.SequenceFile;
import cascading.tap.hadoop.MultiInputFormat;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;

/**
 * Class MultiSinkTap is both a {@link CompositeTap} and {@link SinkTap} that can write to multiple child {@link Tap} instances simultaneously.
 * <p/>
 * It is the counterpart to {@link MultiSourceTap}.
 */
public class MultiSinkTap extends SinkTap<Configuration> implements CompositeTap
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( MultiSinkTap.class );

  /** Field taps */
  private Tap[] taps;
  /** Field tempPath */
  private String tempPath = "__multisink_placeholder" + Integer.toString( (int) ( System.currentTimeMillis() * Math.random() ) );
  /** Field childConfigs */
  private List<Map<String, String>> childConfigs;

  private class MultiSinkCollector extends TupleEntryCollector
    {
    TupleEntryCollector[] collectors;

    public MultiSinkCollector( FlowContext<Configuration> flowContext, Tap... taps ) throws IOException
      {
      collectors = new TupleEntryCollector[taps.length];

      Configuration[] confs = MultiInputFormat.getConfigurations( new Job( flowContext.getConfiguration() ), childConfigs );

      for( int i = 0; i < taps.length; i++ )
        {
        Tap tap = taps[ i ];
        LOG.info( "opening for write: " + tap.toString() );

        collectors[ i ] = tap.openForWrite( new ConfFlowContext( confs[ i ] ) );
        }
      }

    protected void collect( Tuple tuple )
      {
      for( TupleEntryCollector collector : collectors )
        collector.add( tuple );
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
  public boolean isWriteDirect()
    {
    return true;
    }

  @Override
  public Path getPath()
    {
    return new Path( tempPath );
    }

  @Override
  public TupleEntryCollector openForWrite( FlowContext<Configuration> flowContext ) throws IOException
    {
    return new MultiSinkCollector( flowContext, getTaps() );
    }

  @Override
  public void sinkInit( Job job ) throws IOException
    {
    childConfigs = new ArrayList<Map<String, String>>();

    for( int i = 0; i < getTaps().length; i++ )
      {
      Tap tap = getTaps()[ i ];
      Job jobConf = new Job( job.getConfiguration() );

      tap.sinkInit( jobConf );

      childConfigs.add( MultiInputFormat.getConfig( job, jobConf.getConfiguration() ) );
      }
    }

  @Override
  public boolean makeDirs( Job job ) throws IOException
    {
    for( Tap tap : getTaps() )
      {
      if( !tap.makeDirs( job ) )
        return false;
      }

    return true;
    }

  @Override
  public boolean deletePath( Job job ) throws IOException
    {
    for( Tap tap : getTaps() )
      {
      if( !tap.deletePath( job ) )
        return false;
      }

    return true;
    }

  @Override
  public boolean pathExists( Job job ) throws IOException
    {
    for( Tap tap : getTaps() )
      {
      if( !tap.pathExists( job ) )
        return false;
      }

    return true;
    }

  @Override
  public long getPathModified( Job job ) throws IOException
    {
    long modified = getTaps()[ 0 ].getPathModified( job );

    for( int i = 1; i < getTaps().length; i++ )
      modified = Math.max( getTaps()[ i ].getPathModified( job ), modified );

    return modified;
    }

  @Override
  public void sink( TupleEntry tupleEntry, TupleEntryCollector tupleEntryCollector ) throws IOException
    {
    for( int i = 0; i < taps.length; i++ )
      taps[ i ].sink( tupleEntry, ( (MultiSinkCollector) tupleEntryCollector ).collectors[ i ] );
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

    Fields allFields = new Fields( fieldNames.toArray( new Comparable[fieldNames.size()] ) );

    setScheme( new SequenceFile( allFields ) );

    return super.getScheme();
    }

  @Override
  public String toString()
    {
    return "MultiSinkTap[" + ( taps == null ? null : Arrays.asList( taps ) ) + ']';
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
