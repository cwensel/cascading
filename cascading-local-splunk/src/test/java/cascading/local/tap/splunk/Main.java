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

import java.io.IOException;

import cascading.flow.FlowProcess;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;
import com.splunk.HttpService;
import com.splunk.JobExportArgs;
import com.splunk.SSLSecurityProtocol;
import com.splunk.ServiceArgs;

/**
 *
 */
public class Main
  {
  public static void main( String[] args ) throws IOException
    {
    HttpService.setSslSecurityProtocol( SSLSecurityProtocol.TLSv1_2 );

    String host = args[ 0 ];
    String port = args[ 1 ];
    String search = args[ 2 ];
    String earliest = args[ 3 ];
    String latest = args[ 4 ];

    ServiceArgs serviceArgs = new ServiceArgs();

    serviceArgs.putAll( SplunkUtil.loadSplunkRC() );

    serviceArgs.setHost( host );
    serviceArgs.setPort( Integer.valueOf( port ) );

    serviceArgs.setUsername( System.getenv( "SPLUNK_USERNAME" ) );
    serviceArgs.setPassword( System.getenv( "SPLUNK_PASSWORD" ) );

    JobExportArgs exportArgs = new JobExportArgs();
    exportArgs.setEarliestTime( earliest );
    exportArgs.setLatestTime( latest );

    SplunkCSV splunkCSV = new SplunkCSV( Fields.ALL );
    SplunkSearchTap searchTap = new SplunkSearchTap( splunkCSV, serviceArgs, exportArgs, search );

    TupleEntryIterator iterator = searchTap.openForRead( FlowProcess.nullFlowProcess() );

    while( iterator.hasNext() )
      System.out.println( iterator.next().getTuple() );
    }
  }
