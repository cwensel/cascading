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
import cascading.util.PropertyUtil;
import cascading.util.ServiceUtil;
import cascading.util.ShutdownUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class CascadingServices is the root class for pluggable services Cascading can call out to for distributed
 * monitoring and management systems.
 * <p/>
 * Be default all services will be loaded from the jar {@link #DEFAULT_PROPERTIES} resource is found in. If the
 * property {@link #CONTAINER_ENABLED} value is {@code false}, a ClassLoader container will not be created.
 * <p/>
 * For this to work, all service implementation and dependencies must be archived into a single jar.
 * <p/>
 * If any packages in the jar should be excluded, set a comma delimited list of names via the {@link #CONTAINER_EXCLUDE}
 * property.
 *
 * @see CascadingService
 */
public class CascadingServices
  {
  private static final Logger LOG = LoggerFactory.getLogger( CascadingServices.class );

  public static final String DEFAULT_PROPERTIES = "cascading/management/service.properties";
  public static final String CONTAINER_ENABLED = "cascading.management.container.enabled";
  public static final String CONTAINER_EXCLUDE = "cascading.management.container.exclude";

  static Properties defaultProperties;
  static URL libraryURL;
  static String[] exclusions;
  static ServiceUtil serviceUtil;

  Map<Object, Object> properties;

  MetricsService metricsService;
  DocumentService documentService;
  boolean enableContainer;

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

    if( url != null )
      {
      try
        {
        String path = url.toURI().getSchemeSpecificPart();
        int endIndex = path.lastIndexOf( '!' );

        if( endIndex != -1 )
          path = path.substring( 0, endIndex );

        if( path.endsWith( ".jar" ) )
          libraryURL = new URL( path );
        }
      catch( Exception exception )
        {
        LOG.warn( "unable to parse resource library: {}", url, exception );
        }
      }

    exclusions = defaultProperties.getProperty( CONTAINER_EXCLUDE, "" ).split( "," );

    }

  private synchronized ServiceUtil getServiceUtil()
    {
    // allows for a system property to disable the container
    if( serviceUtil == null )
      serviceUtil = ServiceUtil.getInstance( enableContainer ? libraryURL : null, exclusions );

    return serviceUtil;
    }

  public CascadingServices( Map<Object, Object> properties )
    {
    this.properties = properties;
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
        LOG.error( "failed stopping cascading service", throwable );
        throw new CascadeException( "failed stopping cascading service", throwable );
        }
      }
    } );
    }
  }
