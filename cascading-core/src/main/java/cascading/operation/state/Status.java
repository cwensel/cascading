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

  public String getStatus()
    {
    return status;
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

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof Status ) )
      return false;
    if( !super.equals( object ) )
      return false;

    Status status1 = (Status) object;

    if( status != null ? !status.equals( status1.status ) : status1.status != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( status != null ? status.hashCode() : 0 );
    return result;
    }
  }