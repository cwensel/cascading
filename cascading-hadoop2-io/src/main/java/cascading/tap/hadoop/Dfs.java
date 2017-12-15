/*
 * Copyright (c) 2016-2017 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

/**
 * Class Dfs is a {@link cascading.tap.Tap} class that provides access to the Hadoop Distributed File System.
 * <p>
 * Use the {@link URI} constructors to specify a different HDFS cluster than the default.
 */
public class Dfs extends Hfs
  {
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
  protected FileSystem getDefaultFileSystem( Configuration configuration )
    {
    String name = configuration.get( "fs.default.name", "hdfs://localhost:5001/" );

    if( name.equals( "local" ) || name.matches( ".*://.*" ) && !name.startsWith( "hdfs://" ) )
      name = "hdfs://localhost:5001/";
    else if( name.indexOf( '/' ) == -1 )
      name = "hdfs://" + name;

    try
      {
      return FileSystem.get( URI.create( name ), configuration );
      }
    catch( IOException exception )
      {
      throw new TapException( "unable to get handle to get filesystem for: " + name, exception );
      }
    }
  }
