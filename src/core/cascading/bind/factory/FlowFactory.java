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

package cascading.bind.factory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import cascading.bind.Schema;
import cascading.bind.TapResource;
import cascading.cascade.Cascades;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Pipe;
import cascading.tap.MultiSinkTap;
import cascading.tap.MultiSourceTap;
import cascading.tap.Tap;

/**
 * Class FlowFactory is a sub-class of {@link ProcessFactory} that returns Cascading {@link Flow} instances.
 * <p/>
 * This class should be sub-classed when the intent is to create new Cascading {@link Flow} instances.
 * It provides convenience methods for {@link Tap} and {@link Flow} instantiation.
 *
 * @see TapResource
 */
public abstract class FlowFactory extends ProcessFactory<Flow, TapResource>
  {
  protected FlowFactory()
    {
    }

  protected FlowFactory( Properties properties )
    {
    super( properties );
    }

  /**
   * Method getSourceTapFor returns a new {@link Tap} instance for the given name.
   * <p/>
   * First the bound {@link Schema} is looked up, then a Tap is created by the bound
   * {@link TapResource} instance.
   * <p/>
   * If more than one Resource is bound to the given name, a {@link MultiSourceTap}
   * will be returned encapsulating all the resulting Tap instances.
   *
   * @param sourceName
   * @return
   */
  public Tap getSourceTapFor( String sourceName )
    {
    Schema schema = sourceSchemas.get( sourceName );

    if( schema == null )
      throw new IllegalArgumentException( "could not find schema for source name: " + sourceName );

    return getSourceTapFor( sourceName, schema );
    }

  protected Tap getSourceTapFor( String sourceName, Schema schema )
    {
    List<TapResource> resources = getSourceResources( sourceName );

    if( resources.isEmpty() )
      return null;

    Tap[] taps = new Tap[ resources.size() ];

    for( int i = 0; i < resources.size(); i++ )
      taps[ i ] = schema.getTapFor( (TapResource) resources.get( i ) );

    if( taps.length == 1 )
      return taps[ 0 ];

    return new MultiSourceTap( taps );
    }

  /**
   * Method getSinkTapFor returns a new {@link Tap} instance for the given name.
   * <p/>
   * First the bound {@link Schema} is looked up, then a Tap is created by the bound
   * {@link TapResource} instance.
   * <p/>
   * If more than one Resource is bound to the given name, a {@link MultiSinkTap}
   * will be returned encapsulating all the resulting Tap instances.
   *
   * @param sinkName
   * @return
   */
  public Tap getSinkTapFor( String sinkName )
    {
    Schema schema = sinkSchemas.get( sinkName );

    if( schema == null )
      throw new IllegalArgumentException( "could not find schema for sink name: " + sinkName );

    return getSinkTapFor( sinkName, schema );
    }

  protected Tap getSinkTapFor( String sinkName, Schema schema )
    {
    List<TapResource> resources = getSinkResources( sinkName );

    if( resources.isEmpty() )
      return null;

    Tap[] taps = new Tap[ resources.size() ];

    for( int i = 0; i < resources.size(); i++ )
      taps[ i ] = schema.getTapFor( (TapResource) resources.get( i ) );

    if( taps.length == 1 )
      return taps[ 0 ];

    return new MultiSinkTap( taps );
    }

  protected Tap[] getSourceTapsFor( String... sourceNames )
    {
    Tap[] taps = new Tap[ sourceNames.length ];

    for( int i = 0; i < sourceNames.length; i++ )
      {
      taps[ i ] = getSourceTapFor( sourceNames[ i ] );

      if( taps[ i ] == null )
        throw new IllegalArgumentException( "no resource found for source name: " + sourceNames[ i ] );
      }

    return taps;
    }

  protected Tap[] getSinkTapsFor( String... sinkNames )
    {
    Tap[] taps = new Tap[ sinkNames.length ];

    for( int i = 0; i < sinkNames.length; i++ )
      {
      taps[ i ] = getSinkTapFor( sinkNames[ i ] );

      if( taps[ i ] == null )
        throw new IllegalArgumentException( "no resource found for sink name: " + sinkNames[ i ] );
      }

    return taps;
    }

  protected Map<String, Tap> getSourceTapsMap( Pipe... sinkPipes )
    {
    Set<Pipe> sourcePipesSet = new HashSet<Pipe>();

    for( Pipe pipe : sinkPipes )
      Collections.addAll( sourcePipesSet, pipe.getHeads() );

    Pipe[] sourcePipes = sourcePipesSet.toArray( new Pipe[ sourcePipesSet.size() ] );
    String[] names = makeNames( sourcePipes );

    return Cascades.tapsMap( sourcePipes, getSourceTapsFor( names ) );
    }

  protected Map<String, Tap> getSinkTapsMap( Pipe... sinkPipes )
    {
    String[] names = makeNames( sinkPipes );

    return Cascades.tapsMap( sinkPipes, getSinkTapsFor( names ) );
    }

  private String[] makeNames( Pipe[] pipes )
    {
    String[] names = new String[ pipes.length ];

    for( int i = 0; i < pipes.length; i++ )
      names[ i ] = pipes[ i ].getName();

    return names;
    }

  /**
   * Method getFlowConnector returns a new {@link FlowConnector} instance using
   * the parent class {@code properties} values.
   *
   * @return
   */
  protected abstract FlowConnector getFlowConnector();

  /**
   * Method createFlowFrom is a convenience method that returns a new {@link Flow} instance.
   * <p/>
   * After all source and sink resources have been bound, the {@link #create()} implementation
   * should call this method to quickly bind source and sink taps to the given assembly head and tail
   * {@link Pipe} instances.
   *
   * @param name
   * @param tails
   * @return
   */
  protected Flow createFlowFrom( String name, Pipe... tails )
    {
    Map<String, Tap> sources = getSourceTapsMap( tails );
    Map<String, Tap> sinks = getSinkTapsMap( tails );

    return getFlowConnector().connect( name, sources, sinks, tails );
    }
  }
