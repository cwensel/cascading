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

package cascading.test;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.flow.FlowSession;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.scheme.local.TextDelimited;
import cascading.scheme.local.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

/**
 *
 */
public class LocalPlatform extends TestPlatform
  {
  private Properties properties = new Properties();

  @Override
  public void setUp() throws IOException
    {
    }

  @Override
  public Map<Object, Object> getProperties()
    {
    return new Properties( properties );
    }

  @Override
  public void tearDown()
    {
    }

  @Override
  public void copyFromLocal( String inputFile ) throws IOException
    {
    }

  @Override
  public void copyToLocal( String outputFile ) throws IOException
    {
    }

  @Override
  public boolean remoteExists( String outputFile ) throws IOException
    {
    return new File( outputFile ).exists();
    }

  @Override
  public FlowProcess getFlowProcess()
    {
    return new LocalFlowProcess( FlowSession.NULL, (Properties) getProperties() );
    }

  @Override
  public FlowConnector getFlowConnector( Map<Object, Object> properties )
    {
    return new LocalFlowConnector( properties );
    }

  @Override
  public Tap getTextFile( Fields sourceFields, Fields sinkFields, String filename, SinkMode mode )
    {
    if( sourceFields == null )
      return new FileTap( new TextLine(), filename, mode );

    return new FileTap( new TextLine( sourceFields, sinkFields ), filename, mode );
    }

  @Override
  public Tap getDelimitedFile( Fields fields, boolean skipHeader, String delimiter, String quote, Class[] types, String filename, SinkMode mode )
    {
    return new FileTap( new TextDelimited( fields, skipHeader, delimiter, quote, types ), filename, mode );
    }

  @Override
  public Tap getDelimitedFile( Fields fields, boolean skipHeader, boolean writeHeader, String delimiter, String quote, Class[] types, String filename, SinkMode mode )
    {
    return new FileTap( new TextDelimited( fields, skipHeader, writeHeader, delimiter, quote, types ), filename, mode );
    }
  }
