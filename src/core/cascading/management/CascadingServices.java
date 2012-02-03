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

package cascading.management;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Map;
import java.util.Properties;

import cascading.cascade.CascadeException;
import cascading.util.CascadingService;
import cascading.util.ServiceUtil;
import cascading.util.ShutdownUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class CascadingServices is the root class for pluggable services Cascading can call out to for distributed
 * monitoring and management systems.
 * <p/>
 * Be default all services will be loaded from the jar {@link #DEFAULT_PROPERTIES} resource is found in. If the
 * property {@link #USE_CONTAINER} value is {@code false}, a ClassLoader container will not be created.
 * <p/>
 * For this to work, all service implementation and dependencies must be archived into a single jar.
 *
 * @see CascadingService
 */
public class CascadingServices
  {
  private static final Logger LOG = LoggerFactory.getLogger( CascadingServices.class );

  public static final String DEFAULT_PROPERTIES = "cascading/management/service.properties";
  public static final String USE_CONTAINER = "cascading.management.container";

  static Properties defaultProperties;
  static URL libraryURL;

  Map<Object, Object> properties;

  MetricsService metricsService;
  DocumentService documentService;

  static
    {
    defaultProperties = new Properties();

    InputStream input = CascadingServices.class.getClassLoader().getResourceAsStream( DEFAULT_PROPERTIES );

    try
      {
      if( input != null )
        defaultProperties.load( input );
      }
    catch( IOException exception )
      {
      LOG.warn( "unable to load properties from {}", DEFAULT_PROPERTIES, exception );
      }

    URL url = CascadingServices.class.getClassLoader().getResource( DEFAULT_PROPERTIES );

    if( url != null && defaultProperties.getProperty( USE_CONTAINER, "false" ).equalsIgnoreCase( "true" ) )
      {
      try
        {
        String path = url.toURI().getSchemeSpecificPart();
        libraryURL = new URL( path.substring( 0, path.lastIndexOf( '!' ) ) );
        }
      catch( URISyntaxException exception )
        {
        LOG.warn( "unable to parse resource library: {}", url, exception );
        }
      catch( MalformedURLException exception )
        {
        LOG.warn( "unable to parse resource library: {}", url, exception );
        }
      }
    }

  public CascadingServices( Map<Object, Object> properties )
    {
    this.properties = properties;
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
    ClientState clientState = (ClientState) ServiceUtil.loadServiceFrom( defaultProperties, getProperties(), ClientState.STATE_SERVICE_CLASS_PROPERTY, libraryURL );

    if( clientState != null )
      {
      clientState.initialize( this, id );

      return clientState;
      }

    return ClientState.NULL;
    }

  protected MetricsService createMetricsService()
    {
    MetricsService service = (MetricsService) ServiceUtil.loadSingletonServiceFrom( defaultProperties, getProperties(), MetricsService.METRICS_SERVICE_CLASS_PROPERTY, libraryURL );

    if( service != null )
      {
      registerShutdownHook( service );

      return service;
      }

    return new NullMetricsService();
    }

  protected DocumentService createDocumentService()
    {
    DocumentService service = (DocumentService) ServiceUtil.loadSingletonServiceFrom( defaultProperties, getProperties(), DocumentService.DOCUMENT_SERVICE_CLASS_PROPERTY, libraryURL );

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
        LOG.error( "failed stopping cascading service", throwable );
        throw new CascadeException( "failed stopping cascading service", throwable );
        }
      }
    } );
    }
  }
