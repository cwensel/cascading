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

package cascading.management.state;

import java.util.Map;

import cascading.management.CascadingServices;
import cascading.management.DocumentService;
import cascading.management.MetricsService;
import cascading.provider.CascadingService;

/**
 *
 */
public abstract class BaseState implements CascadingService
  {
  private String id;

  MetricsService metricsService = new CascadingServices.NullMetricsService();
  DocumentService documentService = new CascadingServices.NullDocumentService();

  public BaseState()
    {
    }

  @Override
  public boolean isEnabled()
    {
    return metricsService.isEnabled() || documentService.isEnabled();
    }

  @Override
  public void setProperties( Map<Object, Object> properties )
    {
    }

  public void initialize( CascadingServices cascadingServices, String id )
    {
    this.id = id;

    if( cascadingServices == null )
      return;

    metricsService = cascadingServices.getMetricsService();
    documentService = cascadingServices.getDocumentService();
    }

  public void startService()
    {
    metricsService.startService();
    documentService.startService();
    }

  public void stopService()
    {
    // not stopping services, they are singletons,
    // and need to live beyond the shutdown hooks
    }

  String[] getContext( Enum context )
    {
    return getContext( getGroup( context ), context.toString() );
    }

  String getGroup( Enum metric )
    {
    return metric.getClass().getSimpleName();
    }

  abstract String[] getContext( String group, String metric );

  public String getID()
    {
    return id;
    }

  protected void store( String id, Object value )
    {
    documentService.put( id, value );
    }

  protected void setMetric( Enum metric, long value )
    {
    metricsService.set( getContext( metric ), value );
    }

  protected void setMetric( String group, String metric, long value )
    {
    metricsService.set( getContext( group, metric ), value );
    }

  protected void setMetric( String group, String metric, String value )
    {
    metricsService.set( getContext( group, metric ), value );
    }

  private void incrementMetric( Enum metric, int value )
    {
    metricsService.increment( getContext( metric ), value );
    }

  private void incrementMetric( String group, String metric, int value )
    {
    metricsService.increment( getContext( group, metric ), value );
    }

  String[] asArray( String... strings )
    {
    return strings;
    }
  }
