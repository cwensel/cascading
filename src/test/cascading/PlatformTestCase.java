/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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
import cascading.test.LocalPlatform;
import cascading.test.TestPlatform;
import cascading.util.Util;
import org.junit.AfterClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class PlatformTestCase extends CascadingTestCase
  {
  private static final Logger LOG = LoggerFactory.getLogger( PlatformTestCase.class );

  public static final String ROOT_OUTPUT_PATH = "test.output.root";
  static Set<String> allPaths = new HashSet<String>();

  private String rootPath;
  private transient TestPlatform platform = null;

  public PlatformTestCase( boolean useCluster )
    {
    String classname = System.getProperty( TestPlatform.TEST_PLATFORM_CLASSNAME, LocalPlatform.class.getCanonicalName() );

    platform = (TestPlatform) loadClassInstance( classname );
    platform.setUseCluster( useCluster );
//    platform.setProperty( StreamGraph.ERROR_DOT_FILE_PATH, getOutputPath( "streamgraph.dot" )  );
    }

  public PlatformTestCase( boolean useCluster, int numMapTasks, int numReduceTasks )
    {
    this( useCluster );

    if( platform instanceof HadoopPlatform )
      {
      ( (HadoopPlatform) platform ).setNumMapTasks( numMapTasks );
      ( (HadoopPlatform) platform ).setNumReduceTasks( numReduceTasks );
      }
    }

  public PlatformTestCase()
    {
    this( false );
    }

  protected Object loadClassInstance( String classname )
    {
    try
      {
      return getClass().getClassLoader().loadClass( classname ).newInstance();
      }
    catch( InstantiationException exception )
      {
      exception.printStackTrace();
      }
    catch( IllegalAccessException exception )
      {
      exception.printStackTrace();
      }
    catch( ClassNotFoundException exception )
      {
      exception.printStackTrace();
      }

    return null;
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
    return platform.getClass().getSimpleName().replaceAll( "^(.*)Platform$", "$1" ).toLowerCase();
    }

  public static String getTestRoot()
    {
    return System.getProperty( ROOT_OUTPUT_PATH, "build/test/output" );
    }

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

  public void tearDown() throws IOException
    {
    getPlatform().tearDown();
    }

  @AfterClass
  public void copyLocal() throws IOException
    {
    for( String path : allPaths )
      {
      LOG.info( "copying to local {}", path );
      getPlatform().copyToLocal( path );
      }
    }

  public TestPlatform getPlatform()
    {
    return platform;
    }
  }
