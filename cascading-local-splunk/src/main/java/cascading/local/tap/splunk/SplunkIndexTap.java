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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Properties;

import cascading.flow.FlowProcess;
import cascading.tap.SinkMode;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntrySchemeCollector;
import com.splunk.Index;
import com.splunk.JobExportArgs;
import com.splunk.Service;
import com.splunk.ServiceArgs;

/**
 * Class SplunkIndexTap can return all the results from a given index or write to the index.
 * <p>
 * It is currently assumed the index name given is not a pattern/wildcard.
 * <p>
 * In order to source (only) from a search result, see {@link SplunkSearchTap}.
 */
public class SplunkIndexTap extends SplunkTap
  {
  private String indexName;

  public SplunkIndexTap( SplunkScheme scheme, String host, int port, String indexName )
    {
    super( scheme, host, port );
    this.indexName = indexName;
    }

  public SplunkIndexTap( SplunkScheme scheme, ServiceArgs serviceArgs, String indexName )
    {
    super( scheme, serviceArgs );
    this.indexName = indexName;
    }

  public SplunkIndexTap( SplunkScheme scheme, ServiceArgs serviceArgs, JobExportArgs exportArgs, String indexName )
    {
    super( scheme, serviceArgs, exportArgs );
    this.indexName = indexName;
    }

  public SplunkIndexTap( SplunkScheme scheme, ServiceArgs serviceArgs, JobExportArgs exportArgs, SinkMode sinkMode, String indexName )
    {
    super( scheme, serviceArgs, exportArgs, sinkMode );
    this.indexName = indexName;
    }

  public SplunkIndexTap( SplunkScheme scheme, Service service, String indexName )
    {
    super( scheme, service, null );
    this.indexName = indexName;
    }

  public SplunkIndexTap( SplunkScheme scheme, Service service, JobExportArgs exportArgs, String indexName )
    {
    super( scheme, service, exportArgs );
    this.indexName = indexName;
    }

  public SplunkIndexTap( SplunkScheme scheme, Service service, JobExportArgs exportArgs, SinkMode sinkMode, String indexName )
    {
    super( scheme, service, exportArgs, sinkMode );
    this.indexName = indexName;
    }

  @Override
  protected String getSplunkQuery()
    {
    return null;
    }

  @Override
  protected String getSplunkPath()
    {
    return "/" + indexName;
    }

  @Override
  protected String getSearch()
    {
    return String.format( "search index=%s *", indexName );
    }

  @Override
  public TupleEntryCollector openForWrite( FlowProcess<? extends Properties> flowProcess, OutputStream outputStream ) throws IOException
    {
    Index index = getOrCreateIndex();
    Socket socket = index.attach();

    OutputStream stream = new BufferedOutputStream( socket.getOutputStream() )
      {
      @Override
      public void close() throws IOException
        {
        super.close();

        socket.close();
        }
      };

    return new TupleEntrySchemeCollector<Properties, OutputStream>( flowProcess, this, getScheme(), stream, this::getIdentifier );
    }

  protected Index getOrCreateIndex()
    {
    Index index = getIndex();

    if( index == null )
      index = getService().getIndexes().create( indexName );

    return index;
    }

  protected Index getIndex()
    {
    return getService().getIndexes().get( indexName );
    }

  @Override
  public boolean createResource( Properties conf )
    {
    Index index = getOrCreateIndex();

    return index != null;
    }

  @Override
  public boolean deleteResource( Properties conf )
    {
    Index index = getIndex();

    index.remove();

    return true;
    }

  @Override
  public boolean resourceExists( Properties conf ) throws IOException
    {
    return getIndex() != null;
    }

  @Override
  public long getModifiedTime( Properties conf ) throws IOException
    {
    return resourceExists( conf ) ? Long.MAX_VALUE : 0;
    }
  }
