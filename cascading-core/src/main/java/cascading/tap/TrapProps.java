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

package cascading.tap;

import java.util.Properties;

import cascading.property.Props;

/**
 * Class TrapProps is a fluent helper class to set properties which control the behaviour of {@link cascading.tap.Tap}
 * instances used as traps on a given {@link cascading.flow.Flow}.
 * <p/>
 * A Tap trap is used to capture bad data that has triggered an unhandled {@link java.lang.Throwable} within a
 * Cascading {@link cascading.operation.Operation} or {@link cascading.tap.Tap} (either as a source or sink).
 * <p/>
 * When a trap captures a failure, the {@link cascading.tuple.Tuple} arguments to the Operation or Tap will be
 * captured and the Flow will continue executing with the next Tuple in the stream. Otherwise a Flow would
 * typically fail.
 * <p/>
 * Due to the variability of data in the stream at any given point, a Tap trap should be configured with a
 * {@link cascading.scheme.Scheme} that sinks {@link cascading.tuple.Fields#ALL}, or is guaranteed to sink known
 * fields common to all Operations within the branch the trap has been bound too.
 * <p/>
 * Optionally diagnostic information, with the given field names, may be captured along with the argument Tuple.
 * <p/>
 * <ul>
 * <li>{@code element-trace} - the file and line number the failed operation was instantiated</li>
 * <li>{@code throwable-message} - the {@link Throwable#getMessage()} value</li>
 * <li>{@code throwable-stacktrace} - the {@link Throwable#printStackTrace()} value, cleansed</li>
 * </ul>
 * <p/>
 * By default, if the Throwable stacktrace is captured, each line of the trace will be trimmed (to remove the
 * TAB character ({@code \t}) and each new line ({@code \n}) will be replaced with a pipe character ({@code |}).
 * <p/>
 * Each value is prepended to the argument Tuple in the order given above. Since the argument Tuple may vary
 * in size, prepending the diagnostic value deterministically allows for simple grep and sed like commands to be
 * applied to the files.
 * <p/>
 * Trap properties can be applied to a given Flow by calling {@link #buildProperties()} on the properties instance
 * handed to the target {@link cascading.flow.FlowConnector} or directly to any given Tap via the
 * {@link Tap#getConfigDef()} using {@link #setProperties(cascading.property.ConfigDef)}.
 * <p/>
 * It should be noted that traps are not intended for 'flow control' of a data stream. They are for exceptional
 * cases that when reached should not cause a Flow to fail. Flow control (sending known bad data down a different
 * branch) should be part of the application. Traps capture the values that are unaccounted for and cause errors,
 * if missing them doesn't compromise the integrity of the application.
 */
public class TrapProps extends Props
  {
  public static final String RECORD_ELEMENT_TRACE = "cascading.trap.elementtrace.record";
  public static final String RECORD_THROWABLE_MESSAGE = "cascading.trap.throwable.message.record";
  public static final String RECORD_THROWABLE_STACK_TRACE = "cascading.trap.throwable.stacktrace.record";
  public static final String LOG_THROWABLE_STACK_TRACE = "cascading.trap.throwable.stacktrace.log";
  public static final String STACK_TRACE_LINE_TRIM = "cascading.trap.throwable.stacktrace.line.trim";
  public static final String STACK_TRACE_LINE_DELIMITER = "cascading.trap.throwable.stacktrace.line.delimiter";

  protected boolean recordElementTrace = false;
  protected boolean recordThrowableMessage = false;
  protected boolean recordThrowableStackTrace = false;

  protected boolean logThrowableStackTrace = true;

  protected boolean stackTraceTrimLine = true;
  protected String stackTraceLineDelimiter = null;

  public static TrapProps trapProps()
    {
    return new TrapProps();
    }

  public TrapProps()
    {
    }

  /**
   * Method recordAllDiagnostics enables recording of all configurable diagnostic values.
   *
   * @return this
   */
  public TrapProps recordAllDiagnostics()
    {
    recordElementTrace = true;
    recordThrowableMessage = true;
    recordThrowableStackTrace = true;

    return this;
    }

  public boolean isRecordElementTrace()
    {
    return recordElementTrace;
    }

  /**
   * Method setRecordElementTrace will enable recording the element trace value if set to {@code true}.
   * <p/>
   * The default is {@code false}.
   *
   * @param recordElementTrace of type boolean
   * @return this
   */
  public TrapProps setRecordElementTrace( boolean recordElementTrace )
    {
    this.recordElementTrace = recordElementTrace;

    return this;
    }

  public boolean isRecordThrowableMessage()
    {
    return recordThrowableMessage;
    }

  /**
   * Method setRecordThrowableMessage will enable recording the Throwable message value if set to {@code true}.
   * <p/>
   * The default is {@code false}.
   *
   * @param recordThrowableMessage of type boolean
   * @return this
   */
  public TrapProps setRecordThrowableMessage( boolean recordThrowableMessage )
    {
    this.recordThrowableMessage = recordThrowableMessage;

    return this;
    }

  public boolean isRecordThrowableStackTrace()
    {
    return recordThrowableStackTrace;
    }

  /**
   * Method setRecordThrowableStackTrace will enable recording the Throwable stacktrace value if set to {@code true}.
   * <p/>
   * The default is {@code false}.
   *
   * @param recordThrowableStackTrace of type boolean
   * @return this
   */
  public TrapProps setRecordThrowableStackTrace( boolean recordThrowableStackTrace )
    {
    this.recordThrowableStackTrace = recordThrowableStackTrace;

    return this;
    }

  public boolean isLogThrowableStackTrace()
    {
    return logThrowableStackTrace;
    }

  /**
   * Method setLogThrowableStackTrace will disable logging of the Throwable stacktrace value if set to {@code false}.
   * <p/>
   * The default is {@code true}.
   *
   * @param logThrowableStackTrace of type boolean
   * @return this
   */
  public TrapProps setLogThrowableStackTrace( boolean logThrowableStackTrace )
    {
    this.logThrowableStackTrace = logThrowableStackTrace;

    return this;
    }

  public boolean isStackTraceTrimLine()
    {
    return stackTraceTrimLine;
    }

  /**
   * Method setStackTraceTrimLine will disable trimming of whitespace on every recorded stacktrace line if set to
   * {@code false}.
   * <p/>
   * The default is {@code true}.
   *
   * @param stackTraceTrimLine of type boolean
   * @return this
   */
  public TrapProps setStackTraceTrimLine( boolean stackTraceTrimLine )
    {
    this.stackTraceTrimLine = stackTraceTrimLine;

    return this;
    }

  public String getStackTraceLineDelimiter()
    {
    return stackTraceLineDelimiter;
    }

  /**
   * Method setStackTraceLineDelimiter will set the text delimiter used to denote stacktrace lines.
   * <p/>
   * The default is {@code |} (the pipe character).
   *
   * @param stackTraceLineDelimiter of type boolean
   * @return this
   */
  public TrapProps setStackTraceLineDelimiter( String stackTraceLineDelimiter )
    {
    this.stackTraceLineDelimiter = stackTraceLineDelimiter;

    return this;
    }

  @Override
  protected void addPropertiesTo( Properties properties )
    {
    properties.setProperty( RECORD_ELEMENT_TRACE, Boolean.toString( recordElementTrace ) );
    properties.setProperty( RECORD_THROWABLE_MESSAGE, Boolean.toString( recordThrowableMessage ) );
    properties.setProperty( RECORD_THROWABLE_STACK_TRACE, Boolean.toString( recordThrowableStackTrace ) );
    properties.setProperty( LOG_THROWABLE_STACK_TRACE, Boolean.toString( logThrowableStackTrace ) );
    properties.setProperty( STACK_TRACE_LINE_TRIM, Boolean.toString( stackTraceTrimLine ) );

    if( stackTraceLineDelimiter != null )
      properties.setProperty( STACK_TRACE_LINE_DELIMITER, stackTraceLineDelimiter );
    }
  }
