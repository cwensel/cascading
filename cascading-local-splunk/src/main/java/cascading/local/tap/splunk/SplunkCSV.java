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
import java.io.PrintWriter;
import java.util.Properties;
import java.util.TimeZone;

import cascading.flow.FlowProcess;
import cascading.scheme.SinkCall;
import cascading.scheme.local.TextDelimited;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.type.DateType;
import com.splunk.JobExportArgs;

/**
 * Class SplunkCSV is a {@link cascading.scheme.Scheme} that enables CSV export from a Splunk instance.
 * <p>
 * Any given declared source {@link Fields} will be passed to the underlying export request. By default only the
 * default fields will be retrieved, but will be declared as {@link Fields#UNKNOWN} in the final plan.
 * <p>
 * This Scheme may only be used to source data from Splunk.
 * <p>
 * Default fields from web service, but not guaranteed
 * <pre>{@code
 * "_serial"
 * "_time" ->  yyyy-MM-dd HH:mm:ss.SSS z
 * "source"
 * "sourcetype"
 * "host"
 * "index"
 * "splunk_server"
 * "_raw"
 * }</pre>
 * <p>
 */
public class SplunkCSV extends TextDelimited implements SplunkScheme
  {
  public static final DateType DATE_TYPE = new DateType( "yyyy-MM-dd HH:mm:ss.SSS z", TimeZone.getTimeZone( "UTC" ) );
  public static final Fields _TIME = new Fields( "_time", DATE_TYPE );
  public static final Fields _SERIAL = new Fields( "_serial", Long.class ); // optional
  public static final Fields SOURCE = new Fields( "source", String.class );
  public static final Fields SOURCETYPE = new Fields( "sourcetype", String.class );
  public static final Fields HOST = new Fields( "host", String.class );
  public static final Fields INDEX = new Fields( "index", String.class );
  public static final Fields SPLUNK_SERVER = new Fields( "splunk_server", String.class );
  public static final Fields _RAW = new Fields( "_raw", String.class );
  public static final Fields _INDEXTIME = new Fields( "_indextime", long.class );
  public static final Fields _SUBSECOND = new Fields( "_subsecond", float.class );
  public static final Fields TIMESTARTPOS = new Fields( "timestartpos", long.class ); // start pos of time in _raw field
  public static final Fields TIMEENDPOS = new Fields( "timeendpos", long.class ); // end pos of time in _raw field

  /**
   * Typical default fields emitted from an export if no fields specified.
   */
  public static final Fields DEFAULTS = Fields.NONE
    .append( _SERIAL )
    .append( _TIME )
    .append( SOURCE )
    .append( SOURCETYPE )
    .append( HOST )
    .append( INDEX )
    .append( SPLUNK_SERVER )
    .append( _RAW );

  /**
   * All known internal Splunk fields.
   */
  public static final Fields KNOWN = DEFAULTS
    .append( _INDEXTIME )
    .append( _SUBSECOND )
    .append( TIMESTARTPOS )
    .append( TIMEENDPOS );

  /**
   * Instantiates a new SplunkCSV instance.
   */
  public SplunkCSV()
    {
    super( Fields.ALL, true, false, ",", "\"", null );
    }

  /**
   * Instantiates a new SplunkCSV instance that returns the given fields.
   *
   * @param fields the fields
   */
  public SplunkCSV( Fields fields )
    {
    super( fields, true, false, ",", "\"", null );
    }

  @Override
  public boolean isSink()
    {
    return false;
    }

  @Override
  public void sink( FlowProcess<? extends Properties> flowProcess, SinkCall<PrintWriter, OutputStream> sinkCall )
    {
    throw new UnsupportedOperationException( "sinking is not supported" );
    }

  @Override
  public void sourceConfInit( FlowProcess<? extends Properties> flowProcess, Tap<Properties, InputStream, OutputStream> tap, Properties conf )
    {
    super.sourceConfInit( flowProcess, tap, conf );

    JobExportArgs args = new JobExportArgs();

    args.setOutputMode( JobExportArgs.OutputMode.CSV );

    if( getSourceFields().isDefined() )
      {
      Fields sourceFields = getSourceFields();
      String[] fields = new String[ sourceFields.size() ];

      for( int i = 0; i < sourceFields.size(); i++ )
        fields[ i ] = sourceFields.get( i ).toString();

      args.setFieldList( fields );
      }

    conf.put( "args", args );
    }

  @Override
  public Fields retrieveSourceFields( FlowProcess<? extends Properties> process, Tap tap )
    {
    return getSourceFields();
    }
  }
