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

package cascading.bind;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cascading.scheme.Scheme;
import cascading.tap.MultiSinkTap;
import cascading.tap.MultiSourceTap;
import cascading.tap.Tap;
import cascading.tuple.Fields;


/**
 * Class Schema is used to map between 'protocols' and 'formats' to available Cascading Scheme instances.
 * <p/>
 * This is particularly useful when creating data processing applications that need to deal with
 * multiple data formats for data (tab delimited, JSON, thrift, etc) and multiple ways to access the data
 * (HDFS, S3, JDBC, Memcached, etc).
 * <p/>
 * This class is type parameterized for P and F where P represent a 'protocol' and F represents
 * a file or data 'format'. Typically P and F are of type {@link Enum}, but may be any standard class.
 * <p/>
 * It is a common practice to sub-class Schema so that each new class represents a particular abstract
 * data type like 'person' or an Apache server log record.
 *
 * @param <P> a 'protocol' type
 * @param <F> a data 'format' type
 */
public class Schema<P, F>
  {
  String name = getClass().getSimpleName().replaceAll( "Schema$", "" );
  P defaultProtocol;
  Fields sourceFields;
  Fields sinkFields;

  final Map<Pair, Scheme> schemes = new HashMap<Pair, Scheme>();

  private static class Pair<P, F>
    {
    final P protocol;
    final F format;

    private Pair( P protocol, F format )
      {
      this.protocol = protocol;
      this.format = format;
      }

    @Override
    public boolean equals( Object object )
      {
      if( this == object )
        return true;
      if( object == null || getClass() != object.getClass() )
        return false;

      Pair pair = (Pair) object;

      if( format != null ? !format.equals( pair.format ) : pair.format != null )
        return false;
      if( protocol != null ? !protocol.equals( pair.protocol ) : pair.protocol != null )
        return false;

      return true;
      }

    @Override
    public int hashCode()
      {
      int result = protocol != null ? protocol.hashCode() : 0;
      result = 31 * result + ( format != null ? format.hashCode() : 0 );
      return result;
      }

    @Override
    public String toString()
      {
      final StringBuilder sb = new StringBuilder();
      sb.append( "[protocol=" ).append( protocol );
      sb.append( ", format=" ).append( format );
      sb.append( "]" );
      return sb.toString();
      }
    }

  protected Schema( P defaultProtocol )
    {
    if( defaultProtocol == null )
      throw new IllegalArgumentException( "defaultProtocol may not be null" );

    this.defaultProtocol = defaultProtocol;
    }

  public String getName()
    {
    return name;
    }

  protected void setName( String name )
    {
    this.name = name;
    }

  public P getDefaultProtocol()
    {
    return defaultProtocol;
    }

  public Fields getSourceFields()
    {
    return sourceFields;
    }

  public Fields getSinkFields()
    {
    return sinkFields;
    }

  private void setFields( Scheme scheme )
    {
    if( scheme.isSource() )
      {
      if( sourceFields == null )
        sourceFields = scheme.getSourceFields();
      else if( !sourceFields.equals( scheme.getSourceFields() ) )
        throw new IllegalArgumentException( "all schemes added to schema must have the same source fields, expected: " + sourceFields + ", received: " + scheme.getSourceFields() + " in schema: " + getName() );
      }

    if( scheme.isSink() )
      {
      if( sinkFields == null )
        sinkFields = scheme.getSinkFields();
      else if( !sinkFields.equals( scheme.getSinkFields() ) )
        throw new IllegalArgumentException( "all schemes added to schema must have the same sink fields, expected: " + sinkFields + ", received: " + scheme.getSinkFields() + " in schema: " + getName() );
      }
    }

  protected void addSchemeFor( P protocol, F format, Scheme scheme )
    {
    if( protocol == null )
      {
      addSchemeFor( format, scheme );
      return;
      }

    setFields( scheme );

    schemes.put( new Pair<P, F>( protocol, format ), scheme );
    }

  protected void addSchemeFor( F format, Scheme scheme )
    {
    setFields( scheme );

    schemes.put( new Pair<P, F>( defaultProtocol, format ), scheme );
    }

  public Scheme getSchemeFor( P protocol, F format )
    {
    if( protocol == null )
      return getSchemeFor( format );

    return schemes.get( new Pair<P, F>( protocol, format ) );
    }

  public Scheme getSchemeFor( F format )
    {
    return schemes.get( new Pair<P, F>( defaultProtocol, format ) );
    }

  public Collection<Scheme> getAllSchemesFor( F format )
    {
    List<Scheme> found = new ArrayList<Scheme>();

    for( Map.Entry<Pair, Scheme> entry : schemes.entrySet() )
      {
      if( format.equals( entry.getKey().format ) )
        found.add( entry.getValue() );
      }

    return found;
    }

  public boolean containsSchemeFor( F format )
    {
    return !getAllSchemesFor( format ).isEmpty();
    }

  public Tap getTapFor( TapResource<P, F> resource )
    {
    Scheme scheme = getSchemeFor( resource.getProtocol(), resource.getFormat() );

    if( scheme == null )
      throw new IllegalArgumentException( "no scheme found for: " + pair( resource ) + " in schema: " + getName() );

    return resource.createTapFor( scheme );
    }

  public Tap getSourceTapFor( TapResource<P, F>... resources )
    {
    Tap[] taps = new Tap[ resources.length ];

    for( int i = 0; i < resources.length; i++ )
      taps[ i ] = getTapFor( resources[ i ] );

    if( taps.length == 1 )
      return taps[ 0 ];

    return new MultiSourceTap( taps );
    }

  public Tap getSinkTapFor( TapResource<P, F>... resources )
    {
    Tap[] taps = new Tap[ resources.length ];

    for( int i = 0; i < resources.length; i++ )
      taps[ i ] = getTapFor( resources[ i ] );

    if( taps.length == 1 )
      return taps[ 0 ];

    return new MultiSinkTap( taps );
    }

  Pair<P, F> pair( Resource<P, F, ?> resource )
    {
    P protocol = resource.getProtocol();

    if( protocol == null )
      protocol = defaultProtocol;

    return new Pair<P, F>( protocol, resource.getFormat() );
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( object == null || getClass() != object.getClass() )
      return false;

    Schema schema = (Schema) object;

    if( sourceFields != null ? !sourceFields.equals( schema.sourceFields ) : schema.sourceFields != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    return sourceFields != null ? sourceFields.hashCode() : 0;
    }
  }
