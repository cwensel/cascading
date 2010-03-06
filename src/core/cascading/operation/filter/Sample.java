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
import java.util.Random;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.OperationCall;

/**
 * Class Sample is a {@link Filter} that only allows the given fraction of {@link cascading.tuple.Tuple} instances to pass.
 * <p/>
 * Where fraction is between 1 and zero, inclusive. Thus to sample {@code 50%} of the tuples in a stream, use the
 * fraction {@code 0.5}.
 */
public class Sample extends BaseOperation<Random> implements Filter<Random>
  {
  private long seed = System.currentTimeMillis();
  private double fraction = 1.0d;

  /**
   * Creates a new Sample that permits percent Tuples to pass.
   *
   * @param fraction of type double
   */
  @ConstructorProperties({"fraction"})
  public Sample( double fraction )
    {
    this.fraction = fraction;
    }

  /**
   * Creates a new Sample that permits percent Tuples to pass. The given seed value seeds the random number generator.
   *
   * @param seed     of type long
   * @param fraction of type double
   */
  @ConstructorProperties({"seed", "fraction"})
  public Sample( long seed, double fraction )
    {
    this.seed = seed;
    this.fraction = fraction;
    }

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall<Random> operationCall )
    {
    super.prepare( flowProcess, operationCall );

    operationCall.setContext( new Random( seed ) );
    }

  public boolean isRemove( FlowProcess flowProcess, FilterCall<Random> filterCall )
    {
    return !( filterCall.getContext().nextDouble() < fraction );
    }
  }