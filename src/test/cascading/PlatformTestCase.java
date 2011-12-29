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

package cascading;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import cascading.flow.FlowConnector;
import cascading.operation.DebugLevel;
import cascading.test.HadoopPlatform;
import cascading.test.PlatformRunner;
import cascading.test.TestPlatform;
import cascading.util.Util;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
@RunWith(PlatformRunner.class)
public class PlatformTestCase extends CascadingTestCase
  {
  private static final Logger LOG = LoggerFactory.getLogger( PlatformTestCase.class );

  public static final String ROOT_OUTPUT_PATH = "test.output.root";
  static Set<String> allPaths = new HashSet<String>();

  private String rootPath;
  Set<String> currentPaths = new HashSet<String>();

  private transient TestPlatform platform = null;

  private transient boolean useCluster;
  private transient int numMapTasks;
  private transient int numReduceTasks;

  public PlatformTestCase( boolean useCluster )
    {
    this.useCluster = useCluster;
    }

  public PlatformTestCase( boolean useCluster, int numMapTasks, int numReduceTasks )
    {
    this( useCluster );
    this.numMapTasks = numMapTasks;
    this.numReduceTasks = numReduceTasks;
    }

  public PlatformTestCase()
    {
    this( false );
    }

  public void installPlatform( TestPlatform platform )
    {
    this.platform = platform;
    this.platform.setUseCluster( useCluster );

    if( this.platform instanceof HadoopPlatform )
      {
      ( (HadoopPlatform) platform ).setNumMapTasks( numMapTasks );
      ( (HadoopPlatform) platform ).setNumReduceTasks( numReduceTasks );
      }
    }

  public TestPlatform getPlatform()
    {
    return platform;
    }

  protected String getRootPath()
    {
    if( rootPath == null )
      rootPath = Util.join( getPathElements(), "/" );

    return rootPath;
    }

  protected String[] getPathElements()
    {
    return new String[]{getTestRoot(), getPlatformName(), getTestCaseName()};
    }

  public String getOutputPath( String path )
    {
    String result = makeOutputPath( path );

    if( allPaths.contains( result ) )
      throw new IllegalStateException( "path already has been used:" + result );

    allPaths.add( result );
    currentPaths.add( result );

    return result;
    }

  private String makeOutputPath( String path )
    {
    if( path.startsWith( "/" ) )
      return getRootPath() + path;

    return getRootPath() + "/" + path;
    }

  public String getTestCaseName()
    {
    return getClass().getSimpleName().replaceAll( "^(.*)Test.*$", "$1" ).toLowerCase();
    }

  public String getPlatformName()
    {
    return platform.getName();
    }

  public static String getTestRoot()
    {
    return System.getProperty( ROOT_OUTPUT_PATH, "build/test/output" );
    }

  @Before
  public void setUp() throws IOException
    {
    getPlatform().setUp();
    }

  public Map<Object, Object> getProperties()
    {
    return new HashMap<Object, Object>( getPlatform().getProperties() );
    }

  protected void copyFromLocal( String inputFile ) throws IOException
    {
    getPlatform().copyFromLocal( inputFile );
    }

  protected Map<Object, Object> disableDebug()
    {
    Map<Object, Object> properties = getProperties();
    FlowConnector.setDebugLevel( properties, DebugLevel.NONE );

    return properties;
    }

  @After
  public void tearDown() throws IOException
    {
    try
      {
      for( String path : currentPaths )
        {
        LOG.info( "copying to local {}", path );

        if( getPlatform().isUseCluster() && getPlatform().remoteExists( path ) )
          getPlatform().copyToLocal( path );
        }

      currentPaths.clear();
      }
    finally
      {
      getPlatform().tearDown();
      }
    }
  }
