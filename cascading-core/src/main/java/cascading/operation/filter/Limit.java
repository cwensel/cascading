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

package cascading.operation.filter;

import java.beans.ConstructorProperties;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.OperationCall;

/**
 * Class Limit is a {@link Filter} that will limit the number of {@link cascading.tuple.Tuple} instances that it will
 * allow to pass.
 * <br/>
 * Note that the limit value is roughly a suggestion. It attempts to divide the limit number by the number of concurrent
 * tasks, but knowing the number of tasks isn't easily available from configuration information provided to individual
 * tasks. Further, the number of records/lines available to a task may be less than the limit amount.
 * <br/>
 * More consistent results will be received from using {@link Sample}.
 *
 * @see Sample
 */
public class Limit extends BaseOperation<Limit.Context> implements Filter<Limit.Context>
  {
  private long limit = 0;

  public static class Context
    {
    public long limit = 0;
    public long count = 0;

    public boolean increment()
      {
      if( limit == count )
        return true;

      count++;

      return false;
      }
    }

  /**
   * Creates a new Limit class that only allows limit number of Tuple instances to pass.
   *
   * @param limit the number of tuples to let pass
   */
  @ConstructorProperties({"limit"})
  public Limit( long limit )
    {
    this.limit = limit;
    }

  public long getLimit()
    {
    return limit;
    }

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall<Context> operationCall )
    {
    super.prepare( flowProcess, operationCall );

    Context context = new Context();

    operationCall.setContext( context );

    int numTasks = flowProcess.getNumProcessSlices();
    int taskNum = flowProcess.getCurrentSliceNum();

    context.limit = (long) Math.floor( (double) limit / (double) numTasks );

    long remainingLimit = limit % numTasks;

    // evenly divide limits across tasks
    context.limit += taskNum < remainingLimit ? 1 : 0;
    }

  @Override
  public boolean isRemove( FlowProcess flowProcess, FilterCall<Context> filterCall )
    {
    return filterCall.getContext().increment();
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof Limit ) )
      return false;
    if( !super.equals( object ) )
      return false;

    Limit limit1 = (Limit) object;

    if( limit != limit1.limit )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + (int) ( limit ^ limit >>> 32 );
    return result;
    }
  }
