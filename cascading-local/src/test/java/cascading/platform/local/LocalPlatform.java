/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

package cascading.platform.local;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.Properties;

import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.flow.FlowSession;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.platform.TestPlatform;
import cascading.scheme.Scheme;
import cascading.scheme.local.TextDelimited;
import cascading.scheme.local.TextLine;
import cascading.scheme.util.DelimitedParser;
import cascading.scheme.util.FieldTypeResolver;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tap.local.TemplateTap;
import cascading.tuple.Fields;
import org.apache.commons.io.FileUtils;

/**
 * Class LocalPlatform is automatically loaded and injected into a {@link cascading.PlatformTestCase} instance
 * so that all *PlatformTest classes can be tested against the Cascading local mode planner.
 */
public class LocalPlatform extends TestPlatform
  {
  private Properties properties = new Properties();

  {
  properties.putAll( getGlobalProperties() );
  }

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
  public boolean remoteRemove( String outputFile, boolean recursive ) throws IOException
    {
    if( !remoteExists( outputFile ) )
      return true;

    File file = new File( outputFile );

    if( !recursive || !file.isDirectory() )
      return file.delete();

    try
      {
      FileUtils.deleteDirectory( file );
      }
    catch( IOException exception )
      {
      return false;
      }

    return !file.exists();
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
  public Tap getTap( Scheme scheme, String filename, SinkMode mode )
    {
    return new FileTap( scheme, filename, mode );
    }

  @Override
  public Tap getTextFile( Fields sourceFields, Fields sinkFields, String filename, SinkMode mode )
    {
    if( sourceFields == null )
      return new FileTap( new TextLine(), filename, mode );

    return new FileTap( new TextLine( sourceFields, sinkFields ), filename, mode );
    }

  @Override
  public Tap getDelimitedFile( Fields fields, boolean hasHeader, String delimiter, String quote, Class[] types, String filename, SinkMode mode )
    {
    return new FileTap( new TextDelimited( fields, hasHeader, delimiter, quote, types ), filename, mode );
    }

  @Override
  public Tap getDelimitedFile( Fields fields, boolean skipHeader, boolean writeHeader, String delimiter, String quote, Class[] types, String filename, SinkMode mode )
    {
    return new FileTap( new TextDelimited( fields, skipHeader, writeHeader, delimiter, quote, types ), filename, mode );
    }

  @Override
  public Tap getDelimitedFile( String delimiter, String quote, FieldTypeResolver fieldTypeResolver, String filename, SinkMode mode )
    {
    return new FileTap( new TextDelimited( true, new DelimitedParser( delimiter, quote, fieldTypeResolver ) ), filename, mode );
    }

  @Override
  public Tap getTemplateTap( Tap sink, String pathTemplate, int openThreshold )
    {
    return new TemplateTap( (FileTap) sink, pathTemplate, openThreshold );
    }

  @Override
  public Tap getTemplateTap( Tap sink, String pathTemplate, Fields fields, int openThreshold )
    {
    return new TemplateTap( (FileTap) sink, pathTemplate, fields, openThreshold );
    }

  @Override
  public Scheme getTestConfigDefScheme()
    {
    return new LocalConfigDefScheme( new Fields( "line" ) );
    }

  @Override
  public Scheme getTestFailScheme()
    {
    return new LocalFailScheme( new Fields( "line" ) );
    }

  @Override
  public Comparator getLongComparator( boolean reverseSort )
    {
    return new TestLongComparator( reverseSort );
    }

  @Override
  public Comparator getStringComparator( boolean reverseSort )
    {
    return new TestStringComparator( reverseSort );
    }

  @Override
  public String getHiddenTemporaryPath()
    {
    return null;
    }
  }
