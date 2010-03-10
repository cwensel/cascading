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

package cascading.operation.state;

import java.beans.ConstructorProperties;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;

/**
 * Class Counter is a {@link Filter} that increments a given {@link Enum} counter by 1 or by the given {@code increment} value.
 * <p/>
 * Internally, the {@link #isRemove(cascading.flow.FlowProcess, cascading.operation.FilterCall)} method calls
 * {@link FlowProcess#increment(Enum, int)}.
 * <p/>
 * No {@link cascading.tuple.Tuple} instances are ever discarded.
 *
 * @see FlowProcess
 * @see Filter
 */
public class Counter extends BaseOperation implements Filter
  {
  private final Enum counterEnum;
  private final String groupString;
  private final String counterString;
  private final int increment;

  /**
   * Constructor Counter creates a new Counter instance.
   *
   * @param counter of type Enum
   */
  @ConstructorProperties({"counter"})
  public Counter( Enum counter )
    {
    this( counter, 1 );
    }

  /**
   * Constructor Counter creates a new Counter instance.
   *
   * @param counter   of type Enum
   * @param increment of type int
   */
  @ConstructorProperties({"counter", "increment"})
  public Counter( Enum counter, int increment )
    {
    this.counterEnum = counter;
    this.groupString = null;
    this.counterString = null;
    this.increment = increment;
    }

  @ConstructorProperties({"group", "counter"})
  public Counter( String group, String counter )
    {
    this( group, counter, 1 );
    }

  @ConstructorProperties({"group", "counter", "increment"})
  public Counter( String group, String counter, int increment )
    {
    this.counterEnum = null;
    this.groupString = group;
    this.counterString = counter;
    this.increment = increment;
    }

  @Override
  public boolean isRemove( FlowProcess flowProcess, FilterCall filterCall )
    {
    if( counterEnum != null )
      flowProcess.increment( counterEnum, increment );
    else
      flowProcess.increment( groupString, counterString, increment );

    return false;
    }
  }
