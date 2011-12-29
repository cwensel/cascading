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

package cascading.tap.hadoop;

import java.beans.ConstructorProperties;
import java.io.IOException;
import java.net.URI;

import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.TapException;
import cascading.tuple.Fields;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;

/**
 * Class Dfs is a {@link cascading.tap.Tap} class that provides access to the Hadoop Distributed File System.
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

    init( uri );
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

    init( uri );
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

    init( uri );
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

    init( uri );
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

    init( uri );
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

    init( uri );
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

  private void init( URI uri )
    {
    if( !uri.getScheme().equalsIgnoreCase( "hdfs" ) )
      throw new IllegalArgumentException( "uri must use the hdfs scheme" );

    setUriScheme( URI.create( uri.getScheme() + "://" + uri.getAuthority() ) );
    }

  protected void setStringPath( String stringPath )
    {
    if( stringPath.matches( ".*://.*" ) && !stringPath.startsWith( "hdfs://" ) )
      throw new IllegalArgumentException( "uri must use the hdfs scheme" );

    super.setStringPath( stringPath );
    }

  @Override
  protected FileSystem getDefaultFileSystem( JobConf jobConf )
    {
    String name = jobConf.get( "fs.default.name", "hdfs://localhost:5001/" );

    if( name.equals( "local" ) || name.matches( ".*://.*" ) && !name.startsWith( "hdfs://" ) )
      name = "hdfs://localhost:5001/";
    else if( name.indexOf( '/' ) == -1 )
      name = "hdfs://" + name;

    try
      {
      return FileSystem.get( URI.create( name ), jobConf );
      }
    catch( IOException exception )
      {
      throw new TapException( "unable to get handle to get filesystem for: " + name, exception );
      }
    }
  }
