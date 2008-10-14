/*
 * Copyright (c) 2007-2008 Concurrent, Inc. All Rights Reserved.
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

package cascading.operation.filter;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;

/**
 * Class Not is a {@link Filter} class that will logically 'not' (negation) the results of the constructor provided Filter
 * instances.
 * <p/>
 * Logically, if {@link Filter#isRemove(cascading.flow.FlowProcess,cascading.operation.FilterCall)} returns {@code true} for the given instance,
 * this filter will return the opposite, {@code false}.
 *
 * @see And
 * @see Xor
 * @see Not
 */
public class Not extends BaseOperation implements Filter
  {
  /** Field filter */
  private final Filter filter;

  /**
   * Constructor Not creates a new Not instance.
   *
   * @param filter of type Filter
   */
  public Not( Filter filter )
    {
    this.filter = filter;

    if( filter == null )
      throw new IllegalArgumentException( "filter may not be null" );
    }

  /** @see cascading.operation.Filter#isRemove(cascading.flow.FlowProcess,cascading.operation.FilterCall) */
  public boolean isRemove( FlowProcess flowProcess, FilterCall filterCall )
    {
    return !filter.isRemove( flowProcess, filterCall );
    }
  }
