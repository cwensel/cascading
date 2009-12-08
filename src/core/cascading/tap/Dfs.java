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

import java.beans.ConstructorProperties;
import java.io.IOException;
import java.net.URI;

import cascading.scheme.Scheme;
import cascading.tuple.Fields;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;

/**
 * Class Dfs is a {@link Tap} class that provides access to the Hadoop Distributed File System.
 * <p/>
 * Use the {@link URI} constructors to specify a different HDFS cluster than the default.
 */
public class Dfs extends Hfs
  {

  /**
   * Constructor Dfs creates a new Dfs instance.
   *
   * @param fields of type Fields
   * @param uri    of type URI
   */
  @ConstructorProperties({"fields", "uri"})
  public Dfs( Fields fields, URI uri )
    {
    super( fields, uri.getPath() );

    if( !uri.getScheme().equalsIgnoreCase( "hdfs" ) )
      throw new IllegalArgumentException( "uri must use the hdfs scheme" );

    setUriScheme( URI.create( uri.getScheme() + "://" + uri.getAuthority() ) );
    }

  /**
   * Constructor Dfs creates a new Dfs instance.
   *
   * @param fields  of type Fields
   * @param uri     of type URI
   * @param replace of type boolean
   */
  @ConstructorProperties({"fields", "uri", "replace"})
  public Dfs( Fields fields, URI uri, boolean replace )
    {
    super( fields, uri.getPath(), replace );

    if( !uri.getScheme().equalsIgnoreCase( "hdfs" ) )
      throw new IllegalArgumentException( "uri must use the hdfs scheme" );

    setUriScheme( URI.create( uri.getScheme() + "://" + uri.getAuthority() ) );
    }

  /**
   * Constructor Dfs creates a new Dfs instance.
   *
   * @param fields   of type Fields
   * @param uri      of type URI
   * @param sinkMode of type SinkMode
   */
  @ConstructorProperties({"fields", "uri", "sinkMode"})
  public Dfs( Fields fields, URI uri, SinkMode sinkMode )
    {
    super( fields, uri.getPath(), sinkMode );

    if( !uri.getScheme().equalsIgnoreCase( "hdfs" ) )
      throw new IllegalArgumentException( "uri must use the hdfs scheme" );

    setUriScheme( URI.create( uri.getScheme() + "://" + uri.getAuthority() ) );
    }

  /**
   * Constructor Dfs creates a new Dfs instance.
   *
   * @param fields     of type Fields
   * @param stringPath of type String
   */
  @ConstructorProperties({"fields", "stringPath"})
  public Dfs( Fields fields, String stringPath )
    {
    super( fields, stringPath );
    }

  /**
   * Constructor Dfs creates a new Dfs instance.
   *
   * @param fields     of type Fields
   * @param stringPath of type String
   * @param replace    of type boolean
   */
  @ConstructorProperties({"fields", "stringPath", "replace"})
  public Dfs( Fields fields, String stringPath, boolean replace )
    {
    super( fields, stringPath, replace );
    }

  /**
   * Constructor Dfs creates a new Dfs instance.
   *
   * @param fields     of type Fields
   * @param stringPath of type String
   * @param sinkMode   of type SinkMode
   */
  @ConstructorProperties({"fields", "stringPath", "sinkMode"})
  public Dfs( Fields fields, String stringPath, SinkMode sinkMode )
    {
    super( fields, stringPath, sinkMode );
    }

  @ConstructorProperties({"scheme"})
  Dfs( Scheme scheme )
    {
    super( scheme );
    }

  /**
   * Constructor Dfs creates a new Dfs instance.
   *
   * @param scheme of type Scheme
   * @param uri    of type URI
   */
  @ConstructorProperties({"scheme", "uri"})
  public Dfs( Scheme scheme, URI uri )
    {
    super( scheme, uri.getPath() );

    if( !uri.getScheme().equalsIgnoreCase( "hdfs" ) )
      throw new IllegalArgumentException( "uri must use the hdfs scheme" );

    setUriScheme( URI.create( uri.getScheme() + "://" + uri.getAuthority() ) );
    }

  /**
   * Constructor Dfs creates a new Dfs instance.
   *
   * @param scheme  of type Scheme
   * @param uri     of type URI
   * @param replace of type boolean
   */
  @ConstructorProperties({"scheme", "uri", "replace"})
  public Dfs( Scheme scheme, URI uri, boolean replace )
    {
    super( scheme, uri.getPath(), replace );

    if( !uri.getScheme().equalsIgnoreCase( "hdfs" ) )
      throw new IllegalArgumentException( "uri must use the hdfs scheme" );

    setUriScheme( URI.create( uri.getScheme() + "://" + uri.getAuthority() ) );
    }

  /**
   * Constructor Dfs creates a new Dfs instance.
   *
   * @param scheme   of type Scheme
   * @param uri      of type URI
   * @param sinkMode of type SinkMode
   */
  @ConstructorProperties({"scheme", "uri", "sinkMode"})
  public Dfs( Scheme scheme, URI uri, SinkMode sinkMode )
    {
    super( scheme, uri.getPath(), sinkMode );

    if( !uri.getScheme().equalsIgnoreCase( "hdfs" ) )
      throw new IllegalArgumentException( "uri must use the hdfs scheme" );

    setUriScheme( URI.create( uri.getScheme() + "://" + uri.getAuthority() ) );
    }

  /**
   * Constructor Dfs creates a new Dfs instance.
   *
   * @param scheme     of type Scheme
   * @param stringPath of type String
   */
  @ConstructorProperties({"scheme", "stringPath"})
  public Dfs( Scheme scheme, String stringPath )
    {
    super( scheme, stringPath );
    }

  /**
   * Constructor Dfs creates a new Dfs instance.
   *
   * @param scheme     of type Scheme
   * @param stringPath of type String
   * @param replace    of type boolean
   */
  @ConstructorProperties({"scheme", "stringPath", "replace"})
  public Dfs( Scheme scheme, String stringPath, boolean replace )
    {
    super( scheme, stringPath, replace );
    }

  /**
   * Constructor Dfs creates a new Dfs instance.
   *
   * @param scheme     of type Scheme
   * @param stringPath of type String
   * @param sinkMode   of type SinkMode
   */
  @ConstructorProperties({"scheme", "stringPath", "sinkMode"})
  public Dfs( Scheme scheme, String stringPath, SinkMode sinkMode )
    {
    super( scheme, stringPath, sinkMode );
    }

  protected void setStringPath( String stringPath )
    {
    if( stringPath.matches( ".*://.*" ) && !stringPath.startsWith( "hdfs://" ) )
      throw new IllegalArgumentException( "uri must use the hdfs scheme" );

    super.setStringPath( stringPath );
    }

  @Override
  protected FileSystem getDefaultFileSystem( JobConf jobConf ) throws IOException
    {
    String name = jobConf.get( "fs.default.name", "hdfs://localhost:5001/" );

    if( name.equals( "local" ) || name.matches( ".*://.*" ) && !name.startsWith( "hdfs://" ) )
      name = "hdfs://localhost:5001/";
    else if( name.indexOf( '/' ) == -1 )
      name = "hdfs://" + name;

    return FileSystem.get( URI.create( name ), jobConf );
    }
  }
