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

package cascading.flow.local;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.Properties;

import cascading.flow.BaseFlow;
import cascading.flow.FlowDef;
import cascading.flow.FlowException;
import cascading.flow.FlowProcess;
import cascading.flow.planner.PlatformInfo;

/**
 * Class LocalFlow is the local mode specific implementation of a {@link cascading.flow.Flow}.
 * <p/>
 * LocalFlow must be created through a {@link LocalFlowConnector} instance.
 * <p/>
 * If classpath paths are provided on the {@link FlowDef}, the context classloader used to internally urn the current
 * Flow will be swapped out with an URLClassLoader pointing to each element.
 *
 * @see LocalFlowConnector
 */
public class LocalFlow extends BaseFlow<Properties>
  {
  private Properties config;

  public LocalFlow( PlatformInfo platformInfo, Map<Object, Object> properties, Properties config, FlowDef flowDef )
    {
    super( platformInfo, properties, config, flowDef );

    initFromProperties( properties );
    }

  @Override
  protected void initConfig( Map<Object, Object> properties, Properties parentConfig )
    {
    this.config = createConfig( properties, parentConfig );
    }

  @Override
  protected void setConfigProperty( Properties properties, Object key, Object value )
    {
    properties.setProperty( key.toString(), value.toString() );
    }

  @Override
  protected Properties newConfig( Properties defaultConfig )
    {
    return defaultConfig == null ? new Properties() : new Properties( defaultConfig );
    }

  @Override
  public Properties getConfig()
    {
    return config;
    }

  @Override
  public Properties getConfigCopy()
    {
    return new Properties( config );
    }

  @Override
  public Map<Object, Object> getConfigAsProperties()
    {
    return config;
    }

  @Override
  public String getProperty( String key )
    {
    return config.getProperty( key );
    }

  @Override
  public FlowProcess<Properties> getFlowProcess()
    {
    return new LocalFlowProcess( getFlowSession(), config );
    }

  @Override
  protected void internalStart()
    {
    try
      {
      deleteSinksIfReplace();
      deleteTrapsIfReplace();
      }
    catch( IOException exception )
      {
      throw new FlowException( "unable to delete sinks", exception );
      }
    }

  @Override
  protected Thread createFlowThread( String threadName )
    {
    Thread flowThread = super.createFlowThread( threadName );

    flowThread.setContextClassLoader( createClassPathClassloader( flowThread.getContextClassLoader() ) );

    return flowThread;
    }

  private ClassLoader createClassPathClassloader( ClassLoader classLoader )
    {
    if( getClassPath() == null || getClassPath().isEmpty() )
      return classLoader;

    URL[] urls = new URL[ getClassPath().size() ];

    for( int i = 0; i < getClassPath().size(); i++ )
      {
      String path = getClassPath().get( i );
      File file = new File( path ).getAbsoluteFile();

      if( !file.exists() )
        throw new FlowException( "path does not exist: " + file );

      try
        {
        urls[ i ] = file.toURI().toURL();
        }
      catch( MalformedURLException exception )
        {
        throw new FlowException( "bad path: " + file, exception );
        }
      }

    return new URLClassLoader( urls, classLoader );
    }

  @Override
  protected void internalClean( boolean stop )
    {
    }

  @Override
  public boolean stepsAreLocal()
    {
    return false;
    }

  @Override
  protected int getMaxNumParallelSteps()
    {
    return 0;
    }

  @Override
  protected void internalShutdown()
    {
    }
  }
