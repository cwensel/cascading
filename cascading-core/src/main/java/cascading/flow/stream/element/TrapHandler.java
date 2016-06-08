/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.stream.element;

import cascading.flow.FlowElement;
import cascading.flow.FlowProcess;
import cascading.flow.StepCounters;
import cascading.flow.stream.TrapException;
import cascading.flow.stream.duct.DuctException;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tap.TrapProps;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.util.TraceUtil;
import cascading.util.Traceable;
import cascading.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class TrapHandler
  {
  private static final Logger LOG = LoggerFactory.getLogger( TrapHandler.class );

  final FlowProcess flowProcess;
  final FlowElement flowElement;
  final String elementTrace;
  final Tap trap;
  final String trapName;

  boolean recordElementTrace = false;
  boolean recordThrowableMessage = false;
  boolean recordThrowableStackTrace = false;
  boolean logThrowableStackTrace = true;
  boolean stackTraceTrimLine = true;
  String stackTraceLineDelimiter = "|";

  boolean recordAnyDiagnostics;

  Fields diagnosticFields = Fields.UNKNOWN;
  TupleEntry diagnosticEntry;

  public TrapHandler( FlowProcess flowProcess )
    {
    this.flowProcess = flowProcess;
    this.flowElement = null;
    this.trap = null;
    this.trapName = null;
    this.elementTrace = null;
    }

  public TrapHandler( FlowProcess flowProcess, FlowElement flowElement, Tap trap, String trapName )
    {
    this.flowProcess = flowProcess;
    this.flowElement = flowElement;
    this.trap = trap;
    this.trapName = trapName;

    if( flowElement instanceof Traceable )
      this.elementTrace = ( (Traceable) flowElement ).getTrace();
    else
      this.elementTrace = null;

    this.recordElementTrace = flowProcess.getBooleanProperty( TrapProps.RECORD_ELEMENT_TRACE, this.recordElementTrace );
    this.recordThrowableMessage = flowProcess.getBooleanProperty( TrapProps.RECORD_THROWABLE_MESSAGE, this.recordThrowableMessage );
    this.recordThrowableStackTrace = flowProcess.getBooleanProperty( TrapProps.RECORD_THROWABLE_STACK_TRACE, this.recordThrowableStackTrace );
    this.logThrowableStackTrace = flowProcess.getBooleanProperty( TrapProps.LOG_THROWABLE_STACK_TRACE, this.logThrowableStackTrace );
    this.stackTraceLineDelimiter = flowProcess.getStringProperty( TrapProps.STACK_TRACE_LINE_DELIMITER, this.stackTraceLineDelimiter );
    this.stackTraceTrimLine = flowProcess.getBooleanProperty( TrapProps.STACK_TRACE_LINE_TRIM, this.stackTraceTrimLine );

    this.recordAnyDiagnostics = this.recordElementTrace || this.recordThrowableMessage || this.recordThrowableStackTrace;

    Fields fields = new Fields();

    if( this.recordElementTrace )
      fields = fields.append( new Fields( "element-trace" ) );

    if( this.recordThrowableMessage )
      fields = fields.append( new Fields( "throwable-message" ) );

    if( this.recordThrowableStackTrace )
      fields = fields.append( new Fields( "throwable-stacktrace" ) );

    if( fields.size() != 0 )
      this.diagnosticFields = fields;

    this.diagnosticEntry = new TupleEntry( diagnosticFields );
    }

  protected void handleReThrowableException( String message, Throwable throwable )
    {
    LOG.error( message, throwable );

    if( throwable instanceof Error )
      throw (Error) throwable;
    else if( throwable instanceof RuntimeException )
      throw (RuntimeException) throwable;
    else
      throw new DuctException( message, throwable );
    }

  protected void handleException( Throwable exception, TupleEntry tupleEntry )
    {
    handleException( trapName, trap, exception, tupleEntry );
    }

  protected void handleException( String trapName, Tap trap, Throwable throwable, TupleEntry tupleEntry )
    {
    Throwable cause = throwable.getCause();

    if( cause instanceof OutOfMemoryError )
      handleReThrowableException( "caught OutOfMemoryException, will not trap, rethrowing", cause );

    if( cause instanceof TrapException )
      handleReThrowableException( "unable to write trap data, will not trap, rethrowing", cause );

    if( trap == null )
      handleReThrowableException( "caught Throwable, no trap available, rethrowing", throwable );

    TupleEntryCollector trapCollector = flowProcess.getTrapCollectorFor( trap );

    TupleEntry payload;

    if( cause instanceof TapException && ( (TapException) cause ).getPayload() != null )
      {
      payload = new TupleEntry( Fields.UNKNOWN, ( (TapException) cause ).getPayload() );
      }
    else if( tupleEntry != null )
      {
      payload = tupleEntry;
      }
    else
      {
      LOG.error( "failure resolving tuple entry", throwable );
      throw new DuctException( "failure resolving tuple entry", throwable );
      }

    TupleEntry diagnostics = getDiagnostics( throwable );

    if( diagnostics != TupleEntry.NULL ) // prepend diagnostics, payload is variable
      payload = diagnostics.appendNew( payload );

    try
      {
      trapCollector.add( payload );
      }
    catch( Throwable current )
      {
      throw new TrapException( "could not write to trap: " + trap.getIdentifier(), current );
      }

    flowProcess.increment( StepCounters.Tuples_Trapped, 1 );

    if( logThrowableStackTrace )
      LOG.warn( "exception trap on branch: '" + trapName + "', for " + Util.truncate( print( tupleEntry ), 75 ), throwable );
    }

  private TupleEntry getDiagnostics( Throwable throwable )
    {
    if( !recordAnyDiagnostics )
      return TupleEntry.NULL;

    Tuple diagnostics = new Tuple();

    if( recordElementTrace )
      diagnostics.add( elementTrace );

    if( recordThrowableMessage )
      diagnostics.add( throwable.getMessage() );

    if( recordThrowableStackTrace )
      diagnostics.add( TraceUtil.stringifyStackTrace( throwable, stackTraceLineDelimiter, stackTraceTrimLine, -1 ) );

    diagnosticEntry.setTuple( diagnostics );

    return diagnosticEntry;
    }

  private String print( TupleEntry tupleEntry )
    {
    if( tupleEntry == null || tupleEntry.getFields() == null )
      return "[uninitialized]";
    else if( tupleEntry.getTuple() == null )
      return "fields: " + tupleEntry.getFields().printVerbose();
    else
      return "fields: " + tupleEntry.getFields().printVerbose() + " tuple: " + tupleEntry.getTuple().print();
    }
  }


