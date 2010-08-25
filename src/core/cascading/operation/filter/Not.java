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
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.OperationCall;

/**
 * Class Not is a {@link Filter} class that will logically 'not' (negation) the results of the constructor provided Filter
 * instance.
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
  @ConstructorProperties({"filter"})
  public Not( Filter filter )
    {
    this.filter = filter;

    if( filter == null )
      throw new IllegalArgumentException( "filter may not be null" );
    }

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall operationCall )
    {
    filter.prepare( flowProcess, operationCall );
    }

  @Override
  public void cleanup( FlowProcess flowProcess, OperationCall operationCall )
    {
    filter.cleanup( flowProcess, operationCall );
    }

  /** @see cascading.operation.Filter#isRemove(cascading.flow.FlowProcess,cascading.operation.FilterCall) */
  public boolean isRemove( FlowProcess flowProcess, FilterCall filterCall )
    {
    return !filter.isRemove( flowProcess, filterCall );
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof Not ) )
      return false;
    if( !super.equals( object ) )
      return false;

    Not not = (Not) object;

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
