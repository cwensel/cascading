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

package cascading.tap;

import cascading.CascadingException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.util.Util;

/**
 * Class TapException is thrown from {@link Tap} and {@link cascading.scheme.Scheme} subclasses.
 * <p/>
 * Use the payload {@link Tuple} constructor if being thrown from inside a Scheme and which for specific data
 * to be trapped by a failure trap Tap. If the payload is not null, and there is a trap covering the source or sink
 * Tap in question, it will be written to the trap Tap.
 */
public class TapException extends CascadingException
  {
  Tuple payload;

  /** Constructor TapException creates a new TapException instance. */
  public TapException()
    {
    }

  /**
   * Constructor TapException creates a new TapException instance.
   *
   * @param string of type String
   */
  public TapException( String string )
    {
    super( string );
    }

  /**
   * Constructor TapException creates a new TapException instance.
   *
   * @param string    of type String
   * @param throwable of type Throwable
   */
  public TapException( String string, Throwable throwable )
    {
    super( string, throwable );
    }

  /**
   * Constructor TapException creates a new TapException instance.
   *
   * @param string    of type String
   * @param throwable of type Throwable
   * @param payload   of type Tuple
   */
  public TapException( String string, Throwable throwable, Tuple payload )
    {
    super( string, throwable );
    this.payload = payload;
    }

  /**
   * Constructor TapException creates a new TapException instance.
   *
   * @param string  of type String
   * @param payload of type Tuple
   */
  public TapException( String string, Tuple payload )
    {
    super( string );
    this.payload = payload;
    }

  /**
   * Constructor TapException creates a new TapException instance.
   *
   * @param throwable of type Throwable
   */
  public TapException( Throwable throwable )
    {
    super( throwable );
    }

  /**
   * Constructor TapException creates a new TapException instance.
   *
   * @param tap            of type Tap
   * @param incomingFields of type Fields
   * @param selectorFields of type Fields
   * @param throwable      of type Throwable
   */
  public TapException( Tap tap, Fields incomingFields, Fields selectorFields, Throwable throwable )
    {
    super( createMessage( tap, incomingFields, selectorFields ), throwable );
    }

  public Tuple getPayload()
    {
    return payload;
    }

  private static String createMessage( Tap tap, Fields incomingFields, Fields selectorFields )
    {
    String message = "unable to resolve scheme sink selector: " + selectorFields.printVerbose() +
      ", with incoming: " + incomingFields.printVerbose();

    return Util.formatTrace( tap.getScheme(), message );
    }
  }
