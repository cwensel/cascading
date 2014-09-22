/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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

package cascading.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import cascading.operation.BaseOperation;
import cascading.operation.Operation;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.tap.Tap;

/**
 *
 */
public class TraceUtil
  {
  /**
   * The set of regex patterns specifying fully qualified class or package names that serve as boundaries
   * for collecting traces and apiCalls in captureDebugTraceAndApiCall.
   */
  private static final Map<String, Pattern> registeredApiBoundaries = new ConcurrentHashMap<String, Pattern>();

  /**
   * Allows for custom trace fields on Pipe, Tap, and Scheme types
   *
   * @param object
   * @param trace
   */
  public static void setTrace( Object object, String trace )
    {
    Util.setInstanceFieldIfExists( object, "trace", trace );
    }

  /**
   * Allows for custom api call field on Pipe, Tap, and Scheme types
   *
   * @param object
   * @param apiCall
   */
  public static void setApiCall( Object object, String apiCall )
    {
    Util.setInstanceFieldIfExists( object, "apiCall", apiCall );
    }

  private static String formatTrace( Traceable traceable, String message, TraceFormatter formatter )
    {
    if( traceable == null )
      return message;

    String trace = traceable.getTrace();

    if( trace == null )
      return message;

    return formatter.format( trace ) + " " + message;
    }

  public static String formatTrace( final Scheme scheme, String message )
    {
    return formatTrace( scheme, message, new TraceFormatter()
    {
    @Override
    public String format( String trace )
      {
      return "[" + Util.truncate( scheme.toString(), 25 ) + "][" + trace + "]";
      }
    } );
    }

  /**
   * Method formatRawTrace does not include the pipe name
   *
   * @param pipe    of type Pipe
   * @param message of type String
   * @return String
   */
  public static String formatRawTrace( Pipe pipe, String message )
    {
    return formatTrace( pipe, message, new TraceFormatter()
    {
    @Override
    public String format( String trace )
      {
      return "[" + trace + "]";
      }
    } );
    }

  public static String formatTrace( final Pipe pipe, String message )
    {
    return formatTrace( pipe, message, new TraceFormatter()
    {
    @Override
    public String format( String trace )
      {
      return "[" + Util.truncate( pipe.getName(), 25 ) + "][" + trace + "]";
      }
    } );
    }

  public static String formatTrace( final Tap tap, String message )
    {
    return formatTrace( tap, message, new TraceFormatter()
    {
    @Override
    public String format( String trace )
      {
      return "[" + Util.truncate( tap.toString(), 25 ) + "][" + trace + "]";
      }
    } );
    }

  public static String formatTrace( Operation operation, String message )
    {
    if( !( operation instanceof BaseOperation ) )
      return message;

    String trace = ( (BaseOperation) operation ).getTrace();

    if( trace == null )
      return message;

    return "[" + trace + "] " + message;
    }

  public static void captureDebugTraceAndApiCall( Object target )
    {
    StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();

    StackTraceElement candidateTraceElement = null;
    StackTraceElement apiCallElement = null;
    Class<?> tracingBoundary = target.getClass();
    String boundaryClassName = tracingBoundary.getName();

    // walk from the bottom of the stack(which is at the end of the array) upwards towards any boundary.
    // The apiCall is the element at the boundary and the previous stack element is the trace
    for( int i = stackTrace.length - 1; i >= 0; i-- )
      {
      StackTraceElement stackTraceElement = stackTrace[ i ];
      String stackClassName = stackTraceElement.getClassName();

      if( stackClassName != null && ( stackClassName.startsWith( boundaryClassName ) || atApiBoundary( stackTraceElement.toString() ) ) )
        {
        apiCallElement = stackTraceElement;
        break;
        }

      candidateTraceElement = stackTraceElement;
      }

    String trace = candidateTraceElement == null ? "" : candidateTraceElement.toString();
    String apiCall = apiCallElement == null ? "" : apiCallElement.toString();

    setTrace( target, trace );
    setApiCall( target, apiCall );
    }

  private static boolean atApiBoundary( String stackTraceElement )
    {
    for( Pattern boundary : registeredApiBoundaries.values() )
      {
      if( boundary.matcher( stackTraceElement ).matches() )
        return true;
      }

    return false;
    }

  /**
   * Add a regex that serves as a boundary for tracing. That is to say any trace
   * captured by captureDebugTraceAndApiCall will be from a caller that comes higher in the
   * stack than any apiBoundary package or class.
   *
   * @param apiBoundary
   */
  public static void registerApiBoundary( String apiBoundary )
    {
    registeredApiBoundaries.put( apiBoundary, Pattern.compile( apiBoundary ) );
    }

  /**
   * Remove a regex as a boundary for tracing. See registerApiBoundary.
   *
   * @param apiBoundary
   */
  public static void unregisterApiBoundary( String apiBoundary )
    {
    registeredApiBoundaries.remove( apiBoundary );
    }

  private static interface TraceFormatter
    {
    String format( String trace );
    }
  }
