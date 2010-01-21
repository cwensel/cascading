/*
 * Copyright (c) 2007-20010 Concurrent, Inc. All Rights Reserved.
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

package cascading.tap;

/**
 * Enum SinkMode identifies supported modes a Tap may utilize when used as a sink.
 * <p/>
 * Mode KEEP is the default. Typically a failure will result if the resource exists and there is an attempt to
 * write to it.
 * <p/>
 * Mode REPLACE will delete/remove the resource before any attempts to write.
 * <p/>
 * Mode UPDATE will attempt to re-use to the resource, if supported.
 */
public enum SinkMode
  {
    KEEP,
    REPLACE,
    UPDATE,
    @Deprecated APPEND
  }
