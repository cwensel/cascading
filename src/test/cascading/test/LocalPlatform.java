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

package cascading.test;

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
  }
