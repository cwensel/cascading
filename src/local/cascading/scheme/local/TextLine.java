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

package cascading.scheme.local;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Properties;

import cascading.flow.local.LocalFlowProcess;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 *
 */
public class TextLine extends LocalScheme<LineNumberReader, PrintWriter, Void, Void>
  {
  public TextLine()
    {
    super( new Fields( "num", "line" ), Fields.ALL );
    }

  public TextLine( Fields sourceFields )
    {
    super( sourceFields );
    }

  public TextLine( Fields sourceFields, Fields sinkFields )
    {
    super( sourceFields, sinkFields );
    }

  @Override
  public LineNumberReader createInput( FileInputStream inputStream )
    {
    try
      {
      return new LineNumberReader( new InputStreamReader( inputStream, "UTF-8" ) );
      }
    catch( UnsupportedEncodingException exception )
      {
      throw new TapException( exception );
      }
    }

  @Override
  public PrintWriter createOutput( FileOutputStream outputStream )
    {
    try
      {
      return new PrintWriter( new OutputStreamWriter( outputStream, "UTF-8" ) );
      }
    catch( UnsupportedEncodingException exception )
      {
      throw new TapException( exception );
      }
    }

  @Override
  public void sourceConfInit( LocalFlowProcess flowProcess, Tap tap, Properties conf )
    {
    }

  @Override
  public void sinkConfInit( LocalFlowProcess flowProcess, Tap tap, Properties conf )
    {
    }

  @Override
  public boolean source( LocalFlowProcess flowProcess, SourceCall<Void, LineNumberReader> sourceCall ) throws IOException
    {
    // first line is 0, this matches offset being zero, so when throwing out the first line for comments
    int lineNumber = sourceCall.getInput().getLineNumber();
    String line = sourceCall.getInput().readLine();

    if( line == null )
      return false;

    TupleEntry incomingEntry = sourceCall.getIncomingEntry();

    if( getSourceFields().size() == 1 )
      {
      incomingEntry.set( 0, line );
      }
    else
      {
      incomingEntry.set( 0, lineNumber );
      incomingEntry.set( 1, line );
      }

    return true;
    }

  @Override
  public void sink( LocalFlowProcess flowProcess, SinkCall<Void, PrintWriter> sinkCall ) throws IOException
    {
    sinkCall.getOutput().println( sinkCall.getOutgoingEntry().getTuple().toString() );
    }
  }
