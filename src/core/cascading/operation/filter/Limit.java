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

package cascading.operation.filter;

import java.beans.ConstructorProperties;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
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

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall<Context> operationCall )
    {
    super.prepare( flowProcess, operationCall );

    Context context = new Context();

    operationCall.setContext( context );

    HadoopFlowProcess process = (HadoopFlowProcess) flowProcess;

    int numTasks = 0;

    if( process.isMapper() )
      numTasks = process.getCurrentNumMappers();
    else
      numTasks = process.getCurrentNumReducers();

    int taskNum = process.getCurrentTaskNum();

    context.limit = (long) Math.floor( (double) limit / (double) numTasks );

    long remainingLimit = limit % numTasks;

    // evenly divide limits across tasks
    context.limit += taskNum < remainingLimit ? 1 : 0;
    }

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
    result = 31 * result + (int) ( limit ^ ( limit >>> 32 ) );
    return result;
    }
  }
