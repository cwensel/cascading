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

package cascading.tap.hadoop;

import java.beans.ConstructorProperties;
import java.io.IOException;

import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.TapException;
import cascading.tuple.Fields;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;

/**
 * Class Lfs is a {@link cascading.tap.Tap} class that provides access to the Local File System via Hadoop.
 * <p/>
 * Note that using a Lfs {@link cascading.tap.Tap} instance in a {@link cascading.flow.Flow} will force a portion of not the whole Flow to be executed
 * in "local" mode forcing the Flow to execute in the current JVM. Mixing with {@link cascading.tap.hadoop.Dfs} and other Tap
 * types is possible, providing a means to implement complex file/data management functions.
 * <p/>
 * Use {@link cascading.tap.hadoop.Hfs} if you need a Tap instance that inherits the default {@link FileSystem} used by Hadoop.
 */
public class Lfs extends Hfs
  {
  @ConstructorProperties({"scheme"})
  Lfs( Scheme scheme )
    {
    super( scheme );
    }

  /**
   * Constructor Lfs creates a new Lfs instance.
   *
   * @param fields     of type Fields
   * @param stringPath of type String
   */
  @ConstructorProperties({"fields", "stringPath"})
  @Deprecated
  public Lfs( Fields fields, String stringPath )
    {
    super( fields, stringPath );
    }

  /**
   * Constructor Lfs creates a new Lfs instance.
   *
   * @param scheme     of type Scheme
   * @param stringPath of type String
   */
  @ConstructorProperties({"scheme", "stringPath"})
  public Lfs( Scheme scheme, String stringPath )
    {
    super( scheme, stringPath );
    }

  /**
   * Constructor Lfs creates a new Lfs instance.
   *
   * @param scheme     of type Scheme
   * @param stringPath of type String
   * @param replace    of type boolean
   */
  @ConstructorProperties({"scheme", "stringPath", "replace"})
  @Deprecated
  public Lfs( Scheme scheme, String stringPath, boolean replace )
    {
    super( scheme, stringPath, replace );
    }

  /**
   * Constructor Lfs creates a new Lfs instance.
   *
   * @param fields     of type Fields
   * @param stringPath of type String
   * @param sinkMode   of type SinkMode
   */
  @ConstructorProperties({"fields", "stringPath", "sinkMode"})
  @Deprecated
  public Lfs( Fields fields, String stringPath, SinkMode sinkMode )
    {
    super( fields, stringPath, sinkMode );
    }

  /**
   * Constructor Lfs creates a new Lfs instance.
   *
   * @param scheme     of type Scheme
   * @param stringPath of type String
   * @param sinkMode   of type SinkMode
   */
  @ConstructorProperties({"scheme", "stringPath", "sinkMode"})
  public Lfs( Scheme scheme, String stringPath, SinkMode sinkMode )
    {
    super( scheme, stringPath, sinkMode );
    }

  protected void setStringPath( String stringPath )
    {
    if( stringPath.matches( ".*://.*" ) && !stringPath.startsWith( "file://" ) )
      throw new IllegalArgumentException( "uri must use the file scheme" );

    super.setStringPath( stringPath );
    }

  protected FileSystem getFileSystem( JobConf conf )
    {
    try
      {
      return FileSystem.getLocal( conf );
      }
    catch( IOException exception )
      {
      throw new TapException( "unable to get handle to get local filesystem", exception );
      }
    }
  }