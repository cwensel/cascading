/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

package cascading.provider;

import java.net.URL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class ProviderLoader is the base class for loading various "provider" types.
 * <p/>
 * This loader can optinally load a provider implementation within its own ClassLoader using the
 * {@link ChildFirstURLClassLoader}.
 *
 * @see FactoryLoader
 * @see ServiceLoader
 */
public class ProviderLoader<Provider>
  {
  private static final Logger LOG = LoggerFactory.getLogger( ServiceLoader.class );
  URL libraryURL;
  String[] exclusions;

  ClassLoader classLoader;

  public ProviderLoader()
    {
    }

  public ProviderLoader( String[] exclusions, URL libraryURL )
    {
    this.exclusions = exclusions;
    this.libraryURL = libraryURL;
    }

  public Provider createProvider( String className )
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
      Class<Provider> type = (Class<Provider>) getClassLoader().loadClass( className );

      return type.newInstance();
      }
    catch( ClassNotFoundException exception )
      {
      LOG.error( "unable to find service class: {}, with exception: {}", className, exception );
      }
    catch( IllegalAccessException exception )
      {
      LOG.error( "unable to instantiate service class: {}, with exception: {}", className, exception );
      }
    catch( InstantiationException exception )
      {
      LOG.error( "unable to instantiate service class: {}, with exception: {}", className, exception );
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
