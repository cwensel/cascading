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

package cascading.management;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import cascading.util.ServiceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class CascadingServices
  {
  private static final Logger LOG = LoggerFactory.getLogger( CascadingServices.class );

  public static final String DEFAULT_PROPERTIES = "cascading/management/service.properties";

  static Properties defaultProperties;

  Map<Object, Object> properties;

  MetricsService metricsService;
  DocumentService objectService;

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

  public DocumentService getObjectService()
    {
    if( objectService == null )
      objectService = createObjectService();

    return objectService;
    }

  public ClientState createClientState( ClientType clientType, String id )
    {
    ClientState clientState = (ClientState) ServiceUtil.loadServiceFrom( defaultProperties, getProperties(), ClientState.STATE_SERVICE_CLASS_PROPERTY );

    if( clientState != null )
      {
      clientState.initialize( this, clientType, id );

      return clientState;
      }

    return ClientState.NULL;
    }

  protected MetricsService createMetricsService()
    {
    MetricsService service = (MetricsService) ServiceUtil.loadServiceFrom( defaultProperties, getProperties(), MetricsService.METRICS_SERVICE_CLASS_PROPERTY );

    if( service != null )
      return service;

    return new NullMetricsService();
    }

  protected DocumentService createObjectService()
    {
    DocumentService service = (DocumentService) ServiceUtil.loadServiceFrom( defaultProperties, getProperties(), DocumentService.DOCUMENT_SERVICE_CLASS_PROPERTY );

    if( service != null )
      return service;

    return new NullDocumentService();
    }
  }
