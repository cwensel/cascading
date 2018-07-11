/*
 * Copyright (c) 2016-2018 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading.flow.stream;

import cascading.CascadingException;

/**
 * Exception {@link StopDataNotificationException} is thrown to notify the current Flow to stop processing data new, but to complete
 * processing data that has already passed the throwing {@link cascading.operation.Operation}.
 * <p>
 * This feature is only supported in Cascading Local Mode.
 */
public class StopDataNotificationException extends CascadingException
  {
  /** Constructor StopDataNotificationException creates a new StopDataNotificationException instance. */
  public StopDataNotificationException()
    {
    }

  /**
   * Constructor StopDataNotificationException creates a new StopDataNotificationException instance.
   *
   * @param string of type String
   */
  public StopDataNotificationException( String string )
    {
    super( string );
    }
  }
