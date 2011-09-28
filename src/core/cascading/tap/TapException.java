/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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
import cascading.util.Util;

/** Class TapException is thrown from {@link Tap} subclasses. */
public class TapException extends CascadingException
  {
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

  private static String createMessage( Tap tap, Fields incomingFields, Fields selectorFields )
    {
    String message = "unable to resolve scheme sink selector: " + selectorFields.printVerbose() +
      ", with incoming: " + incomingFields.printVerbose();

    return Util.formatTrace( tap.getScheme(), message );
    }
  }
