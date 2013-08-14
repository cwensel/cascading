/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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
 * {@link FlowProcess#increment(Enum, long)}.
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

  public Enum getCounterEnum()
    {
    return counterEnum;
    }

  public String getGroupString()
    {
    return groupString;
    }

  public String getCounterString()
    {
    return counterString;
    }

  public int getIncrement()
    {
    return increment;
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

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof Counter ) )
      return false;
    if( !super.equals( object ) )
      return false;

    Counter counter = (Counter) object;

    if( increment != counter.increment )
      return false;
    if( counterEnum != null ? !counterEnum.equals( counter.counterEnum ) : counter.counterEnum != null )
      return false;
    if( counterString != null ? !counterString.equals( counter.counterString ) : counter.counterString != null )
      return false;
    if( groupString != null ? !groupString.equals( counter.groupString ) : counter.groupString != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( counterEnum != null ? counterEnum.hashCode() : 0 );
    result = 31 * result + ( groupString != null ? groupString.hashCode() : 0 );
    result = 31 * result + ( counterString != null ? counterString.hashCode() : 0 );
    result = 31 * result + increment;
    return result;
    }
  }
