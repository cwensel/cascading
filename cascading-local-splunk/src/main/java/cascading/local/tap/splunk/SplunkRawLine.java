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
import cascading.scheme.local.TextLine;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import com.splunk.JobExportArgs;

/**
 * Class SplunkRawLine is a {@link cascading.scheme.Scheme} that enables RAW export from a Splunk instance.
 * <p>
 * It is a sub-class of the {@link TextLine} Scheme, so is intended to sink and source full lines of (un-parsed) text.
 * See {@link SplunkRawDelimited} for inline parsing of log lines.
 * <p>
 * This Tap does not support reading/writing headers.
 */
public class SplunkRawLine extends TextLine implements SplunkScheme
  {
  /**
   * Instantiates a new SplunkRawLine.
   */
  public SplunkRawLine()
    {
    }

  /**
   * Instantiates a new SplunkRawLine.
   *
   * @param sourceFields the source fields
   */
  public SplunkRawLine( Fields sourceFields )
    {
    super( sourceFields );
    }

  /**
   * Instantiates a new SplunkRawLine.
   *
   * @param sourceFields the source fields
   * @param charsetName  the charset name
   */
  public SplunkRawLine( Fields sourceFields, String charsetName )
    {
    super( sourceFields, charsetName );
    }

  /**
   * Instantiates a new SplunkRawLine.
   *
   * @param sourceFields the source fields
   * @param sinkFields   the sink fields
   */
  public SplunkRawLine( Fields sourceFields, Fields sinkFields )
    {
    super( sourceFields, sinkFields );
    }

  /**
   * Instantiates a new SplunkRawLine.
   *
   * @param sourceFields the source fields
   * @param sinkFields   the sink fields
   * @param charsetName  the charset name
   */
  public SplunkRawLine( Fields sourceFields, Fields sinkFields, String charsetName )
    {
    super( sourceFields, sinkFields, charsetName );
    }

  @Override
  public void sourceConfInit( FlowProcess<? extends Properties> flowProcess, Tap<Properties, InputStream, OutputStream> tap, Properties conf )
    {
    super.sourceConfInit( flowProcess, tap, conf );

    JobExportArgs args = new JobExportArgs();

    args.setOutputMode( JobExportArgs.OutputMode.RAW );

    conf.put( "args", args );
    }
  }
