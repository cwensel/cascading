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

package cascading.util;

import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ServiceUtil
  {
  private static final Logger LOG = LoggerFactory.getLogger( ServiceUtil.class );

  private Map<String, CascadingService> singletons = new HashMap<String, CascadingService>();
  private static ServiceUtil serviceUtil;

  ClassLoader classLoader;
  URL libraryURL;
  String[] exclusions;

  public synchronized static ServiceUtil getInstance( URL libraryURL, String[] exclusions )
    {
    if( serviceUtil == null )
      serviceUtil = new ServiceUtil( libraryURL, exclusions );

    return serviceUtil;
    }

  public synchronized static void releaseSingletonsAndDestroy()
    {
    if( serviceUtil != null )
      serviceUtil.releaseSingletonServices();

    serviceUtil = null;
    }

  ServiceUtil( URL libraryURL, String[] exclusions )
    {
    this.libraryURL = libraryURL;
    this.exclusions = exclusions;
    }

  public synchronized CascadingService loadSingletonServiceFrom( Properties defaultProperties, Map<Object, Object> properties, String property )
    {
    String className = getStringProperty( defaultProperties, properties, property );

    if( !singletons.containsKey( className ) )
      singletons.put( className, createService( properties, className ) );

    return singletons.get( className );
    }

  private static String getStringProperty( Properties defaultProperties, Map<Object, Object> properties, String property )
    {
    return PropertyUtil.getProperty( properties, property, defaultProperties.getProperty( property ) );
    }

  public synchronized Collection<CascadingService> releaseSingletonServices()
    {
    try
      {
      return Collections.unmodifiableCollection( singletons.values() );
      }
    finally
      {
      singletons.clear();
      }
    }

  public CascadingService loadServiceFrom( Properties defaultProperties, Map<Object, Object> properties, String property )
    {
    String className = getStringProperty( defaultProperties, properties, property );

    return createService( properties, className );
    }

  public CascadingService createService( Map<Object, Object> properties, String className )
    {
    // test for ant style token escapes
    if( className == null || className.isEmpty() )
      return null;

    if( className.startsWith( "@" ) && className.endsWith( "@" ) )
      {
      LOG.warn( "invalid classname: {}", className );
      return null;
      }

    try
      {
      Class type = getClassLoader().loadClass( className );

      CascadingService service = (CascadingService) type.newInstance();

      service.setProperties( properties );

      return service;
      }
    catch( ClassNotFoundException exception )
      {
      LOG.error( "unable to find service class: {}", className, exception );
      }
    catch( IllegalAccessException exception )
      {
      LOG.error( "unable to instantiate service class: {}", className, exception );
      }
    catch( InstantiationException exception )
      {
      LOG.error( "unable to instantiate service class: {}", className, exception );
      }

    return null;
    }

  private synchronized ClassLoader getClassLoader()
    {
    if( classLoader != null )
      return classLoader;

    if( libraryURL == null )
      {
      classLoader = Thread.currentThread().getContextClassLoader();
      }
    else
      {
      LOG.info( "loading services from library: {}", libraryURL );

      classLoader = new ChildFirstURLClassLoader( exclusions, libraryURL );
      }

    return classLoader;
    }
  }
