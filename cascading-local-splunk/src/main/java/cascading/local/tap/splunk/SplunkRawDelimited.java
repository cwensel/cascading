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
import java.util.Properties;

import cascading.flow.FlowProcess;
import cascading.scheme.local.TextDelimited;
import cascading.scheme.util.DelimitedParser;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import com.splunk.JobExportArgs;

/**
 * Class SplunkRawDelimited is a {@link cascading.scheme.Scheme} that enables RAW export from a Splunk instance.
 * <p>
 * It is a sub-class of the {@link TextDelimited} Scheme, so is intended to sink and source delimited lines of (parsed) text.
 * See {@link SplunkRawLine} for handling of full (unparsed) log lines.
 * <p>
 * This Tap does not support reading/writing headers.
 */
public class SplunkRawDelimited extends TextDelimited implements SplunkScheme
  {
  /**
   * Instantiates a new SplunkRawDelimited.
   */
  public SplunkRawDelimited()
    {
    }

  /**
   * Instantiates a new SplunkRawDelimited.
   *
   * @param delimitedParser the delimited parser
   */
  public SplunkRawDelimited( DelimitedParser delimitedParser )
    {
    super( delimitedParser );
    }

  /**
   * Instantiates a new SplunkRawDelimited.
   *
   * @param fields the fields
   */
  public SplunkRawDelimited( Fields fields )
    {
    super( fields );
    }

  /**
   * Instantiates a new SplunkRawDelimited.
   *
   * @param fields    the fields
   * @param delimiter the delimiter
   */
  public SplunkRawDelimited( Fields fields, String delimiter )
    {
    super( fields, delimiter );
    }

  /**
   * Instantiates a new SplunkRawDelimited.
   *
   * @param fields    the fields
   * @param delimiter the delimiter
   * @param types     the types
   */
  public SplunkRawDelimited( Fields fields, String delimiter, Class[] types )
    {
    super( fields, delimiter, types );
    }

  /**
   * Instantiates a new SplunkRawDelimited.
   *
   * @param fields    the fields
   * @param delimiter the delimiter
   * @param quote     the quote
   * @param types     the types
   */
  public SplunkRawDelimited( Fields fields, String delimiter, String quote, Class[] types )
    {
    super( fields, delimiter, quote, types );
    }

  /**
   * Instantiates a new SplunkRawDelimited.
   *
   * @param fields    the fields
   * @param delimiter the delimiter
   * @param quote     the quote
   * @param types     the types
   * @param safe      the safe
   */
  public SplunkRawDelimited( Fields fields, String delimiter, String quote, Class[] types, boolean safe )
    {
    super( fields, delimiter, quote, types, safe );
    }

  /**
   * Instantiates a new SplunkRawDelimited.
   *
   * @param fields    the fields
   * @param delimiter the delimiter
   * @param quote     the quote
   */
  public SplunkRawDelimited( Fields fields, String delimiter, String quote )
    {
    super( fields, delimiter, quote );
    }

  @Override
  public void sourceConfInit( FlowProcess<? extends Properties> flowProcess, Tap<Properties, InputStream, OutputStream> tap, Properties conf )
    {
    super.sourceConfInit( flowProcess, tap, conf );

    JobExportArgs args = new JobExportArgs();

    args.setOutputMode( JobExportArgs.OutputMode.RAW );

    conf.put( "args", args );
    }

  @Override
  public Fields retrieveSourceFields( FlowProcess<? extends Properties> process, Tap tap )
    {
    return getSourceFields();
    }
  }
