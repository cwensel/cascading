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
import java.util.LinkedHashMap;
import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.OperationCall;
import cascading.tuple.Tuple;

/**
 * Class FilterPartialDuplicates is a {@link Filter} that is used to remove observed duplicates from the tuple stream.
 * <p/>
 * Use this class typically in tandem with a {@link cascading.operation.aggregator.First}
 * {@link cascading.operation.Aggregator} in order to improve de-duping performance by removing as many values
 * as possible before the intermediate {@link cascading.pipe.GroupBy} operator.
 * <p/>
 * The {@code threshold} value is used to maintain a LRU of a constant size. If more than threshold unique values
 * are seen, the oldest cached values will be removed from the cache.
 *
 * @see cascading.pipe.assembly.Unique
 */
public class FilterPartialDuplicates extends BaseOperation<LinkedHashMap<Tuple, Object>> implements Filter<LinkedHashMap<Tuple, Object>>
  {
  private int threshold = 10000;

  /** Constructor FilterPartialDuplicates creates a new FilterPartialDuplicates instance. */
  public FilterPartialDuplicates()
    {
    }

  /**
   * Constructor FilterPartialDuplicates creates a new FilterPartialDuplicates instance.
   *
   * @param threshold of type int
   */
  @ConstructorProperties({"threshold"})
  public FilterPartialDuplicates( int threshold )
    {
    this.threshold = threshold;
    }

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall<LinkedHashMap<Tuple, Object>> operationCall )
    {
    operationCall.setContext( new LinkedHashMap<Tuple, Object>( threshold, 0.75f, true )
    {
    @Override
    protected boolean removeEldestEntry( Map.Entry eldest )
      {
      return size() > threshold;
      }
    } );
    }

  @Override
  public boolean isRemove( FlowProcess flowProcess, FilterCall<LinkedHashMap<Tuple, Object>> filterCall )
    {
    // we assume its more painful to create lots of tuple copies vs comparisons
    Tuple args = filterCall.getArguments().getTuple();

    if( filterCall.getContext().containsKey( args ) )
      return true;

    filterCall.getContext().put( filterCall.getArguments().getTupleCopy(), null );

    return false;
    }

  @Override
  public void cleanup( FlowProcess flowProcess, OperationCall<LinkedHashMap<Tuple, Object>> operationCall )
    {
    operationCall.setContext( null );
    }
  }
