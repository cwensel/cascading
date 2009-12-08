/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
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
import cascading.operation.OperationCall;

/**
 * Class Status is a {@link cascading.operation.Filter} that sets the current {@link FlowProcess} 'status' on
 * the first {@link cascading.tuple.Tuple} it sees.
 * <p/>
 * Internally, the {@link #isRemove(cascading.flow.FlowProcess, cascading.operation.FilterCall)} method calls
 * {@link cascading.flow.FlowProcess#setStatus(String)}.
 * <p/>
 * No {@link cascading.tuple.Tuple} instances are ever discarded.
 *
 * @see FlowProcess
 * @see Filter
 */
public class Status extends BaseOperation<Boolean> implements Filter<Boolean>
  {
  /** Field status */
  private final String status;

  /**
   * Constructor Status creates a new Status instance.
   *
   * @param status of type String
   */
  @ConstructorProperties({"status"})
  public Status( String status )
    {
    this.status = status;
    }

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall<Boolean> operationCall )
    {
    operationCall.setContext( false );
    }

  @Override
  public boolean isRemove( FlowProcess flowProcess, FilterCall<Boolean> filterCall )
    {
    if( !filterCall.getContext() )
      {
      filterCall.setContext( true );
      flowProcess.setStatus( status );
      }

    return false;
    }

  }