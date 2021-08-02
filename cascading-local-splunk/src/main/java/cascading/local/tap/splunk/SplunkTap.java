/*
 * Copyright (c) 2018 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading.local.tap.splunk;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Properties;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleEntrySchemeIterator;
import com.splunk.JobExportArgs;
import com.splunk.Service;
import com.splunk.ServiceArgs;

/**
 *
 */
public abstract class SplunkTap extends Tap<Properties, InputStream, OutputStream>
  {
  private ServiceArgs serviceArgs;
  private Service service;
  private JobExportArgs exportArgs;

  public SplunkTap( SplunkScheme scheme, String host, int port )
    {
    super( (Scheme<Properties, InputStream, OutputStream, ?, ?>) scheme, SinkMode.UPDATE );

    ServiceArgs serviceArgs = new ServiceArgs();

    serviceArgs.setHost( host );
    serviceArgs.setPort( port );

    this.serviceArgs = serviceArgs;
    }

  public SplunkTap( SplunkScheme scheme )
    {
    super( (Scheme<Properties, InputStream, OutputStream, ?, ?>) scheme, SinkMode.UPDATE );
    }

  public SplunkTap( SplunkScheme scheme, SinkMode sinkMode )
    {
    super( (Scheme<Properties, InputStream, OutputStream, ?, ?>) scheme, sinkMode );
    }

  public SplunkTap( SplunkScheme scheme, ServiceArgs serviceArgs, SinkMode sinkMode )
    {
    super( (Scheme<Properties, InputStream, OutputStream, ?, ?>) scheme, sinkMode );
    this.serviceArgs = serviceArgs;
    }

  public SplunkTap( SplunkScheme scheme, ServiceArgs serviceArgs )
    {
    super( (Scheme<Properties, InputStream, OutputStream, ?, ?>) scheme, SinkMode.UPDATE );
    this.serviceArgs = serviceArgs;
    }

  public SplunkTap( SplunkScheme scheme, ServiceArgs serviceArgs, JobExportArgs exportArgs, SinkMode sinkMode )
    {
    super( (Scheme<Properties, InputStream, OutputStream, ?, ?>) scheme, sinkMode );
    this.serviceArgs = serviceArgs;
    this.exportArgs = exportArgs;
    }

  public SplunkTap( SplunkScheme scheme, ServiceArgs serviceArgs, JobExportArgs exportArgs )
    {
    super( (Scheme<Properties, InputStream, OutputStream, ?, ?>) scheme, SinkMode.UPDATE );
    this.serviceArgs = serviceArgs;
    this.exportArgs = exportArgs;
    }

  public SplunkTap( SplunkScheme scheme, Service service, JobExportArgs exportArgs, SinkMode sinkMode )
    {
    super( (Scheme<Properties, InputStream, OutputStream, ?, ?>) scheme, sinkMode );
    this.service = service;
    this.exportArgs = exportArgs;
    }

  public SplunkTap( SplunkScheme scheme, Service service, JobExportArgs exportArgs )
    {
    super( (Scheme<Properties, InputStream, OutputStream, ?, ?>) scheme, SinkMode.UPDATE );
    this.service = service;
    this.exportArgs = exportArgs;
    }

  protected Service getService()
    {
    if( service != null )
      return service;

    service = Service.connect( serviceArgs );

    return service;
    }

  @Override
  public String getIdentifier()
    {
    URI splunk;

    try
      {
      splunk = new URI( getService().getScheme().equals( "https" ) ? "splunks" : "splunk", null, getService().getHost(), getService().getPort(), getSplunkPath(), getSplunkQuery(), null );
      }
    catch( URISyntaxException exception )
      {
      throw new TapException( "could not create identifier", exception );
      }

    return splunk.toString();
    }

  protected abstract String getSplunkQuery();

  protected abstract String getSplunkPath();

  @Override
  public TupleEntryIterator openForRead( FlowProcess<? extends Properties> flowProcess, InputStream inputStream )
    {
    Properties properties = new Properties();

    sourceConfInit( flowProcess, properties );

    JobExportArgs exportArgs = new JobExportArgs();

    if( this.exportArgs != null )
      exportArgs.putAll( this.exportArgs );

    // grab the provided args object and add
    if( properties.get( "args" ) != null )
      exportArgs.putAll( (Map<? extends String, ?>) properties.get( "args" ) );

    if( !exportArgs.containsKey( "timeout" ) )
      exportArgs.setTimeout( 60 );

    String search = getSearch();

    inputStream = getService().export( search, exportArgs );

    return new TupleEntrySchemeIterator<Properties, InputStream>( flowProcess, this, getScheme(), inputStream, this::getIdentifier );
    }

  protected abstract String getSearch();
  }
