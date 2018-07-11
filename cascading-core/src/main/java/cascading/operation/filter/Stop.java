/*
 * Copyright (c) 2016-2018 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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
import cascading.flow.stream.StopDataNotificationException;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.OperationCall;
import cascading.util.Traceable;

/**
 * Class Stop is a {@link Filter} class that will force the current pipeline to stop reading data.
 * <p>
 * When the nested filter returns {@code true} on {@link Filter#isRemove(FlowProcess, FilterCall)}, a
 * {@link StopDataNotificationException} will be thrown.
 * <p>
 * Currently this only works reliably using the LocalFlowConnector.
 */
public class Stop extends BaseOperation implements Filter
  {
  /** Field filter */
  private final Filter filter;

  /**
   * Constructor Not creates a new Not instance.
   *
   * @param filter of type Filter
   */
  @ConstructorProperties({"filter"})
  public Stop( Filter filter )
    {
    this.filter = filter;

    if( filter == null )
      throw new IllegalArgumentException( "filter may not be null" );
    }

  public Filter getFilter()
    {
    return filter;
    }

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall operationCall )
    {
    filter.prepare( flowProcess, operationCall );
    }

  @Override
  public boolean isRemove( FlowProcess flowProcess, FilterCall filterCall )
    {
    if( filter.isRemove( flowProcess, filterCall ) )
      throw new StopDataNotificationException( "data stopped on filter: " + getFilterString() );

    return false;
    }

  protected String getFilterString()
    {
    String string = filter.toString();

    if( filter instanceof Traceable )
      string += " @ " + ( (Traceable) filter ).getTrace();

    return string;
    }

  @Override
  public void cleanup( FlowProcess flowProcess, OperationCall operationCall )
    {
    filter.cleanup( flowProcess, operationCall );
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof Stop ) )
      return false;
    if( !super.equals( object ) )
      return false;

    Stop not = (Stop) object;

    if( filter != null ? !filter.equals( not.filter ) : not.filter != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( filter != null ? filter.hashCode() : 0 );
    return result;
    }
  }
