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

import cascading.flow.FlowProcess;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 * A TextLine is a type of {@link cascading.scheme.Scheme} for plain text files. Files are broken into
 * lines. Either line-feed or carriage-return are used to signal end of line.
 * <p/>
 * By default, this scheme returns a {@link cascading.tuple.Tuple} with two fields, "num" and "line". Where "num"
 * is the line number for "line".
 * <p/>
 * Many of the constructors take both "sourceFields" and "sinkFields". sourceFields denote the field names
 * to be used instead of the names "num" and "line". sinkFields is a selector and is by default {@link Fields#ALL}.
 * Any available field names can be given if only a subset of the incoming fields should be used.
 * <p/>
 * If a {@link Fields} instance is passed on the constructor as sourceFields having only one field, the return tuples
 * will simply be the "line" value using the given field name.
 * <p/>
 * Note that TextLine will concatenate all the Tuple values for the selected fields with a TAB delimiter before
 * writing out the line.
 */
public class TextLine extends LocalScheme<LineNumberReader, PrintWriter, Void, Void>
  {
  /**
   * Creates a new TextLine instance that sources "num" and "line" fields, and sinks all incoming fields, where
   * "num" is the line number of the line in the input file.
   */
  public TextLine()
    {
    super( new Fields( "num", "line" ), Fields.ALL );
    }

  /**
   * Creates a new TextLine instance. If sourceFields has one field, only the text line will be returned in the
   * subsequent tuples.
   *
   * @param sourceFields of Fields
   */
  public TextLine( Fields sourceFields )
    {
    super( sourceFields );

    verify( sourceFields );
    }

  /**
   * Creates a new TextLine instance. If sourceFields has one field, only the text line will be returned in the
   * subsequent tuples.
   *
   * @param sourceFields of Fields
   * @param sinkFields   of Fields
   */
  public TextLine( Fields sourceFields, Fields sinkFields )
    {
    super( sourceFields, sinkFields );

    verify( sourceFields );
    }

  private void verify( Fields sourceFields )
    {
    if( sourceFields.size() < 1 || sourceFields.size() > 2 )
      throw new IllegalArgumentException( "this scheme requires either one or two source fields, given [" + sourceFields + "]" );
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
  public void sourceConfInit( FlowProcess<Properties> flowProcess, Tap<FlowProcess<Properties>, Properties, LineNumberReader, PrintWriter> tap, Properties conf )
    {
    }

  @Override
  public void sinkConfInit( FlowProcess<Properties> flowProcess, Tap<FlowProcess<Properties>, Properties, LineNumberReader, PrintWriter> tap, Properties conf )
    {
    }

  @Override
  public boolean source( FlowProcess<Properties> flowProcess, SourceCall<Void, LineNumberReader> sourceCall ) throws IOException
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
  public void sink( FlowProcess<Properties> flowProcess, SinkCall<Void, PrintWriter> sinkCall ) throws IOException
    {
    sinkCall.getOutput().println( sinkCall.getOutgoingEntry().getTuple().toString() );
    }
  }
