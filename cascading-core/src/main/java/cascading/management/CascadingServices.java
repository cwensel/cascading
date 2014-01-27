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

package cascading.management;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import cascading.cascade.CascadeException;
import cascading.management.state.ClientState;
import cascading.property.PropertyUtil;
import cascading.provider.CascadingService;
import cascading.provider.ServiceLoader;
import cascading.util.ShutdownUtil;
import cascading.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class CascadingServices is the root class for pluggable services Cascading can call out to for distributed
 * monitoring and management systems.
 * <p/>
 * Be default all services will be loaded from the jar {@code cascading/management/service.properties}
 * ({@link #DEFAULT_PROPERTIES}) resource is found in. If the
 * property {@link #CONTAINER_ENABLED} value is {@code false}, a ClassLoader container will not be created.
 * <p/>
 * For this to work, all service implementation and dependencies must be archived into a single jar.
 * <p/>
 * If any packages in the jar should be excluded, set a comma delimited list of names via the {@link #CONTAINER_EXCLUDE}
 * property.
 * <p/>
 * If the file {@code cascading-service.properties} ({@link CascadingServices#CASCADING_SERVICES}) is found in the
 * CLASSPATH, the {@code cascading.management.service.jar} property value will be used to search for
 * {@code cascading/management/service.properties} resource.
 *
 * @see CascadingService
 */
public class CascadingServices
  {
  private static final Logger LOG = LoggerFactory.getLogger( CascadingServices.class );

  public static final String CASCADING_SERVICES = "cascading-service.properties";
  public static final String CASCADING_SERVICES_JAR = "cascading.management.service.jar";

  public static final String DEFAULT_PROPERTIES = "cascading/management/service.properties";
  public static final String CONTAINER_ENABLED = "cascading.management.container.enabled";
  public static final String CONTAINER_EXCLUDE = "cascading.management.container.exclude";

  static Properties defaultProperties;
  static URL libraryURL;
  static String[] exclusions;

  Map<Object, Object> properties;

  MetricsService metricsService;
  DocumentService documentService;
  boolean enableContainer;

  static
    {
    ClassLoader classLoader = CascadingServices.class.getClassLoader();

    // load all properties from cascading-services.properties
    defaultProperties = loadProperties( new Properties(), CASCADING_SERVICES, classLoader );

    libraryURL = getLibraryURL();

    if( libraryURL != null )
      classLoader = new URLClassLoader( new URL[]{libraryURL} );

    // load additional, if only, properties from file in resource jar
    defaultProperties = loadProperties( new Properties( defaultProperties ), DEFAULT_PROPERTIES, classLoader );

    // if resources jar is on primary classpath, find the jar so we can isolate it when loading services
    if( libraryURL == null )
      libraryURL = parseLibraryURL( classLoader, DEFAULT_PROPERTIES );

    exclusions = Util.removeNulls( defaultProperties.getProperty( CONTAINER_EXCLUDE, "" ).split( "," ) );
    }

  private static URL parseLibraryURL( ClassLoader classLoader, String resource )
    {
    URL url = classLoader.getResource( resource );

    if( url != null )
      {
      try
        {
        String path = url.toURI().getSchemeSpecificPart();
        int endIndex = path.lastIndexOf( '!' );

        if( endIndex != -1 )
          path = path.substring( 0, endIndex );

        if( path.endsWith( ".jar" ) )
          return new URL( path );
        }
      catch( Exception exception )
        {
        LOG.warn( "unable to parse resource library: {}", url, exception );
        }
      }

    return null;
    }

  private static URL getLibraryURL()
    {
    String property = defaultProperties.getProperty( CASCADING_SERVICES_JAR );

    if( property == null )
      return null;

    try
      {
      URI uri = URI.create( property );

      if( !uri.isAbsolute() )
        uri = new URI( "file", uri.getAuthority(), uri.getPath(), uri.getQuery(), uri.getFragment() );

      return uri.toURL();
      }
    catch( Exception exception )
      {
      LOG.warn( "property: {}, has invalid URL value: {}", CASCADING_SERVICES_JAR, property );
      }

    return null;
    }

  private static Properties loadProperties( Properties properties, String resource, ClassLoader classLoader )
    {
    InputStream input = classLoader.getResourceAsStream( resource );

    try
      {
      if( input != null )
        {
        URL url = parseLibraryURL( classLoader, resource );

        if( url != null )
          LOG.info( "loading properties: {}, from jar: {}", resource, url );
        else
          LOG.info( "loading properties: {}", resource );

        properties.load( input );
        }
      }
    catch( IOException exception )
      {
      LOG.warn( "unable to load properties from {}", resource, exception );
      }

    return properties;
    }

  private synchronized ServiceLoader getServiceUtil()
    {
    return ServiceLoader.getInstance( enableContainer ? libraryURL : null, exclusions );
    }

  public CascadingServices( Map<Object, Object> properties )
    {
    this.properties = PropertyUtil.createProperties( properties, defaultProperties );
    this.enableContainer = PropertyUtil.getProperty( properties, CONTAINER_ENABLED, defaultProperties.getProperty( CONTAINER_ENABLED, "false" ) ).equalsIgnoreCase( "true" );
    }

  private Map<Object, Object> getProperties()
    {
    return properties;
    }

  public MetricsService getMetricsService()
    {
    if( metricsService == null )
      metricsService = createMetricsService();

    return metricsService;
    }

  public DocumentService getDocumentService()
    {
    if( documentService == null )
      documentService = createDocumentService();

    return documentService;
    }

  public ClientState createClientState( String id )
    {
    ClientState clientState = (ClientState) getServiceUtil().loadServiceFrom( defaultProperties, getProperties(), ClientState.STATE_SERVICE_CLASS_PROPERTY );

    if( clientState != null )
      {
      clientState.initialize( this, id );

      return clientState;
      }

    return ClientState.NULL;
    }

  protected MetricsService createMetricsService()
    {
    MetricsService service = (MetricsService) getServiceUtil().loadSingletonServiceFrom( defaultProperties, getProperties(), MetricsService.METRICS_SERVICE_CLASS_PROPERTY );

    if( service != null )
      {
      registerShutdownHook( service );

      return service;
      }

    return new NullMetricsService();
    }

  protected DocumentService createDocumentService()
    {
    DocumentService service = (DocumentService) getServiceUtil().loadSingletonServiceFrom( defaultProperties, getProperties(), DocumentService.DOCUMENT_SERVICE_CLASS_PROPERTY );

    if( service != null )
      {
      registerShutdownHook( service );

      return service;
      }

    return new NullDocumentService();
    }

  private void registerShutdownHook( final CascadingService service )
    {
    if( service == null )
      return;

    ShutdownUtil.addHook( new ShutdownUtil.Hook()
    {
    @Override
    public Priority priority()
      {
      return Priority.SERVICE_PROVIDER;
      }

    @Override
    public void execute()
      {
      try
        {
        service.stopService();
        }
      catch( Throwable throwable )
        {
        // safe to throw exception here so message is logged
        LOG.error( "failed stopping cascading service", throwable );
        throw new CascadeException( "failed stopping cascading service", throwable );
        }
      }
    } );
    }

  /** Class NullDocumentService provides a null implementation. */
  public static class NullDocumentService implements DocumentService
    {
    @Override
    public boolean isEnabled()
      {
      return false;
      }

    @Override
    public void setProperties( Map<Object, Object> properties )
      {
      }

    @Override
    public void startService()
      {
      }

    @Override
    public void stopService()
      {
      }

    @Override
    public void put( String key, Object object )
      {
      }

    @Override
    public void put( String type, String key, Object object )
      {
      }

    @Override
    public Map get( String type, String key )
      {
      return null;
      }

    @Override
    public boolean supportsFind()
      {
      return false;
      }

    @Override
    public List<Map<String, Object>> find( String type, String[] query )
      {
      return null;
      }
    }

  /** Class NullMetricsService provides a null implementation. */
  public static class NullMetricsService implements MetricsService
    {
    @Override
    public boolean isEnabled()
      {
      return false;
      }

    @Override
    public void increment( String[] context, int amount )
      {
      }

    @Override
    public void set( String[] context, String value )
      {
      }

    @Override
    public void set( String[] context, int value )
      {
      }

    @Override
    public void set( String[] context, long value )
      {
      }

    @Override
    public String getString( String[] context )
      {
      return null;
      }

    @Override
    public int getInt( String[] context )
      {
      return 0;
      }

    @Override
    public long getLong( String[] context )
      {
      return 0;
      }

    @Override
    public boolean compareSet( String[] context, String isValue, String toValue )
      {
      return true;
      }

    @Override
    public boolean compareSet( String[] context, int isValue, int toValue )
      {
      return true;
      }

    @Override
    public boolean compareSet( String[] context, long isValue, long toValue )
      {
      return true;
      }

    @Override
    public void setProperties( Map<Object, Object> properties )
      {
      }

    @Override
    public void startService()
      {
      }

    @Override
    public void stopService()
      {
      }
    }
  }
