/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static cascading.property.PropertyUtil.getStringProperty;

/**
 * Class ServiceLoader is an implementation of a {@link ProviderLoader} and is used to load
 * {@link CascadingService} instances used by internal frameworks.
 *
 * @see CascadingService
 */
public class ServiceLoader extends ProviderLoader<CascadingService>
  {
  private static ServiceLoader serviceLoader;

  private Map<String, CascadingService> singletons = new HashMap<String, CascadingService>();

  public synchronized static ServiceLoader getInstance( URL libraryURL, String[] exclusions )
    {
    if( serviceLoader == null )
      serviceLoader = new ServiceLoader( libraryURL, exclusions );

    return serviceLoader;
    }

  public synchronized static void releaseSingletonsAndDestroy()
    {
    if( serviceLoader != null )
      serviceLoader.releaseSingletonServices();

    serviceLoader = null;
    }

  ServiceLoader( URL libraryURL, String[] exclusions )
    {
    super( exclusions, libraryURL );
    }

  public synchronized CascadingService loadSingletonServiceFrom( Properties defaultProperties, Map<Object, Object> properties, String property )
    {
    String className = getStringProperty( defaultProperties, properties, property );

    if( !singletons.containsKey( className ) )
      singletons.put( className, loadServiceFrom( defaultProperties, properties, property ) );

    return singletons.get( className );
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

    CascadingService service = createProvider( className );

    if( service != null )
      service.setProperties( properties );

    return service;
    }
  }
