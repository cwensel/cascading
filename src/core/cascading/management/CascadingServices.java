/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
 */

package cascading.management;

import java.util.Map;

import cascading.util.ServiceUtil;

/**
 *
 */
public class CascadingServices
  {
  Map<Object, Object> properties;

  MetricsService metricsService;
  DocumentService objectService;

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

  protected MetricsService createMetricsService()
    {
    MetricsService service = (MetricsService) ServiceUtil.loadServiceFrom( getProperties(), MetricsService.METRICS_SERVICE_CLASS_PROPERTY );

    if( service != null )
      return service;

    return new NullMetricsService();
    }

  protected DocumentService createObjectService()
    {
    DocumentService service = (DocumentService) ServiceUtil.loadServiceFrom( getProperties(), DocumentService.DOCUMENT_SERVICE_CLASS_PROPERTY );

    if( service != null )
      return service;

    return new NullDocumentService();
    }
  }
