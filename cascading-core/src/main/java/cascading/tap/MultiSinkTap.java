/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.FlowProcess;
import cascading.scheme.NullScheme;
import cascading.scheme.Scheme;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class MultiSinkTap is both a {@link cascading.tap.CompositeTap} and {@link cascading.tap.SinkTap} that can write to
 * multiple child {@link cascading.tap.Tap} instances simultaneously.
 * <p/>
 * It is the counterpart to {@link cascading.tap.MultiSourceTap}.
 * <p/>
 * Note all child Tap instances may or may not have the same declared Fields. In the case they do not, all
 * sink fields will be merged into a single Fields instance via {@link Fields#merge(cascading.tuple.Fields...)}.
 */
public class MultiSinkTap<Child extends Tap, Config, Output> extends SinkTap<Config, Output> implements CompositeTap<Child>
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( MultiSinkTap.class );

  /** Field taps */
  private final Child[] taps;
  /** Field tempPath */
  private final String tempPath = "__multisink_placeholder_" + Util.createUniqueID();
  /** Field childConfigs */
  private List<Map<String, String>> childConfigs;

  private class MultiSinkCollector extends TupleEntryCollector
    {
    TupleEntryCollector[] collectors;

    public <C extends Config> MultiSinkCollector( FlowProcess<C> flowProcess, Tap<Config,?,?>... taps ) throws IOException
      {
      super( Fields.asDeclaration( getSinkFields() ) );

      collectors = new TupleEntryCollector[ taps.length ];

      C conf = flowProcess.getConfigCopy();

      for( int i = 0; i < taps.length; i++ )
        {
        C mergedConf = childConfigs == null ? conf : flowProcess.mergeMapIntoConfig( conf, childConfigs.get( i ) );
        Tap<Config,?,?> tap = taps[ i ];
        LOG.info( "opening for write: {}", tap.toString() );

        collectors[ i ] = tap.openForWrite( flowProcess.copyWith( mergedConf ), null );
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
  public MultiSinkTap( Child... taps )
    {
    this.taps = taps;
    }

  protected Child[] getTaps()
    {
    return taps;
    }

  @Override
  public Iterator<Child> getChildTaps()
    {
    return Arrays.asList( getTaps() ).iterator();
    }

  @Override
  public long getNumChildTaps()
    {
    return getTaps().length;
    }

  @Override
  public String getIdentifier()
    {
    return tempPath;
    }

  @Override
  public void presentSinkFields( FlowProcess<? extends Config> flowProcess, Fields fields )
    {
    for( Tap child : getTaps() )
      child.presentSinkFields( flowProcess, fields );
    }

  @Override
  public TupleEntryCollector openForWrite( FlowProcess<? extends Config> flowProcess, Output output ) throws IOException
    {
    return new MultiSinkCollector( flowProcess, getTaps() );
    }

  @Override
  public void sinkConfInit( FlowProcess<? extends Config> flowProcess, Config conf )
    {
    bridge( flowProcess, conf );
    }

  private void bridge( FlowProcess flowProcess, Object conf )
    {
    childConfigs = new ArrayList<>();

    for( int i = 0; i < getTaps().length; i++ )
      {
      Tap tap = getTaps()[ i ];
      Object newConfig =  flowProcess.copyConfig( conf );

      tap.sinkConfInit( flowProcess, newConfig );

      childConfigs.add( flowProcess.diffConfigIntoMap( conf, newConfig ) );
      }
    }

  @Override
  public boolean createResource( Config conf ) throws IOException
    {
    for( Tap tap : getTaps() )
      {
      if( !tap.createResource( conf ) )
        return false;
      }

    return true;
    }

  @Override
  public boolean deleteResource( Config conf ) throws IOException
    {
    for( Tap tap : getTaps() )
      {
      if( !tap.deleteResource( conf ) )
        return false;
      }

    return true;
    }

  @Override
  public boolean commitResource( Config conf ) throws IOException
    {
    for( Tap tap : getTaps() )
      {
      if( !tap.commitResource( conf ) )
        return false;
      }

    return true;
    }

  @Override
  public boolean rollbackResource( Config conf ) throws IOException
    {
    for( Tap tap : getTaps() )
      {
      if( !tap.rollbackResource( conf ) )
        return false;
      }

    return true;
    }

  @Override
  public boolean resourceExists( Config conf ) throws IOException
    {
    for( Tap tap : getTaps() )
      {
      if( !tap.resourceExists( conf ) )
        return false;
      }

    return true;
    }

  @Override
  public long getModifiedTime( Config conf ) throws IOException
    {
    long modified = getTaps()[ 0 ].getModifiedTime( conf );

    for( int i = 1; i < getTaps().length; i++ )
      modified = Math.max( getTaps()[ i ].getModifiedTime( conf ), modified );

    return modified;
    }

  @Override
  public Scheme getScheme()
    {
    if( super.getScheme() != null )
      return super.getScheme();

    Set<Fields> fields = new HashSet<Fields>();

    for( Tap child : getTaps() )
      fields.add( child.getSinkFields() );

    // if all schemes have the same sink fields, the just use the scheme
    if( fields.size() == 1 )
      {
      setScheme( getTaps()[ 0 ].getScheme() );
      return super.getScheme();
      }

    Fields allFields = Fields.merge( fields.toArray( new Fields[ fields.size() ] ) );

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
