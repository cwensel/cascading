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

import java.io.OutputStream;
import java.util.Properties;

import cascading.flow.FlowProcess;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;
import com.splunk.JobExportArgs;
import com.splunk.Service;
import com.splunk.ServiceArgs;

/**
 * Class SplunkSearchTap can return all the results from a given search query.
 * <p>
 * This Tap cannot be used to sink data into a Splunk cluster, see {@link SplunkIndexTap}.
 */
public class SplunkSearchTap extends SplunkTap
  {
  private String search;

  /**
   * Instantiates a new SplunkSearchTap.
   *
   * @param scheme the scheme
   * @param host   the host
   * @param port   the port
   * @param search the search
   */
  public SplunkSearchTap( SplunkScheme scheme, String host, int port, String search )
    {
    super( scheme, host, port );
    this.search = search;
    }

  /**
   * Instantiates a new SplunkSearchTap.
   *
   * @param scheme      the scheme
   * @param serviceArgs the args
   * @param search      the search
   */
  public SplunkSearchTap( SplunkScheme scheme, ServiceArgs serviceArgs, String search )
    {
    super( scheme, serviceArgs );
    this.search = search;
    }

  /**
   * Instantiates a new SplunkSearchTap.
   *
   * @param scheme      the scheme
   * @param serviceArgs the args
   * @param exportArgs  the export args
   * @param search      the search
   */
  public SplunkSearchTap( SplunkScheme scheme, ServiceArgs serviceArgs, JobExportArgs exportArgs, String search )
    {
    super( scheme, serviceArgs, exportArgs );
    this.search = search;
    }

  /**
   * Instantiates a new SplunkSearchTap.
   *
   * @param scheme  the scheme
   * @param service the service
   * @param search  the search
   */
  public SplunkSearchTap( SplunkScheme scheme, Service service, String search )
    {
    super( scheme, service, null );
    this.search = search;
    }

  @Override
  protected String getSplunkQuery()
    {
    return search;
    }

  @Override
  protected String getSplunkPath()
    {
    return "/";
    }

  @Override
  protected String getSearch()
    {
    return String.format( "search %s", search );
    }

  @Override
  public Fields getSinkFields()
    {
    throw new UnsupportedOperationException( "unable to sink tuple streams via a SourceTap instance" );
    }

  @Override
  public final boolean isSink()
    {
    return false;
    }

  @Override
  public boolean deleteResource( Properties conf )
    {
    throw new UnsupportedOperationException( "unable to delete files via a SourceTap instance" );
    }

  @Override
  public void sinkConfInit( FlowProcess<? extends Properties> flowProcess, Properties conf )
    {
    throw new UnsupportedOperationException( "unable to source tuple streams via a SourceTap instance" );
    }

  @Override
  public boolean prepareResourceForWrite( Properties conf )
    {
    throw new UnsupportedOperationException( "unable to prepare resource for write via a SourceTap instance" );
    }

  @Override
  public boolean createResource( Properties conf )
    {
    throw new UnsupportedOperationException( "unable to make dirs via a SourceTap instance" );
    }

  @Override
  public boolean commitResource( Properties conf )
    {
    throw new UnsupportedOperationException( "unable to commit resource via a SourceTap instance" );
    }

  @Override
  public boolean rollbackResource( Properties conf )
    {
    throw new UnsupportedOperationException( "unable to rollback resource via a SourceTap instance" );
    }

  @Override
  public boolean resourceExists( Properties conf )
    {
    return true;
    }

  @Override
  public long getModifiedTime( Properties conf )
    {
    return Long.MAX_VALUE;
    }

  @Override
  public TupleEntryCollector openForWrite( FlowProcess<? extends Properties> flowProcess, OutputStream output )
    {
    throw new UnsupportedOperationException( "unable to open for write via a SourceTap instance" );
    }
  }
