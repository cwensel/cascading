/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

/**
 * Enum SinkMode identifies supported modes a Tap may utilize when used as a sink.
 * <p/>
 * Mode KEEP is the default. Typically a failure will result if the resource exists and there is an attempt to
 * write to it.
 * <p/>
 * Mode REPLACE will delete/remove the resource before any attempts to write.
 * <p/>
 * Mode UPDATE will attempt to re-use to the resource, if supported, and update data in place.
 */
public enum SinkMode
  {
    KEEP,
    REPLACE,
    UPDATE
  }
