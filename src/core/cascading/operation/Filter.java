/*
 * Copyright (c) 2007-2010 Concurrent, Inc. All Rights Reserved.
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

package cascading.operation;

import cascading.flow.FlowProcess;

/** Interface Filter marks a given {@link Operation} as a filter, as opposed to being a {@link Function}. */
public interface Filter<C> extends Operation<C>
  {
  /**
   * Method isRemove returns true if input should be removed from the tuple stream.
   *
   * @param flowProcess of type FlowProcess
   * @param filterCall  of type FilterCall
   * @return boolean
   */
  boolean isRemove( FlowProcess flowProcess, FilterCall<C> filterCall );
  }
  