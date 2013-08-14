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
 * <p/>
 * By default, the seed is created at random on the constructor. This implies every branch using the Sample
 * filter will return the same random stream based on that seed. So if this Sample instance is distributed
 * into multiple systems against the same data, the result will be the same tuple stream. The alternative
 * would be to make this Operation "not safe". See {@link cascading.operation.Operation#isSafe()}.
 * <p/>
 * Conversely, if the same stream of random data is require across application executions, set the seed manually.
 * <p/>
 * The seed is generated from the following code:
 * <p/>
 * {@code System.identityHashCode(this) * 2654435761L ^ System.currentTimeMillis()}
 * <p/>
 * Override {@link #makeSeed()} to customize.
 */
public class Sample extends BaseOperation<Random> implements Filter<Random>
  {
  private long seed = 0;
  private double fraction = 1.0d;

  /**
   * Creates a new Sample that permits percent Tuples to pass.
   *
   * @param fraction of type double
   */
  @ConstructorProperties({"fraction"})
  public Sample( double fraction )
    {
    this.seed = makeSeed();
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

  public long getSeed()
    {
    return seed;
    }

  public double getFraction()
    {
    return fraction;
    }

  protected long makeSeed()
    {
    return System.identityHashCode( this ) * 2654435761L ^ System.currentTimeMillis();
    }

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall<Random> operationCall )
    {
    super.prepare( flowProcess, operationCall );

    operationCall.setContext( new Random( seed ) );
    }

  @Override
  public boolean isRemove( FlowProcess flowProcess, FilterCall<Random> filterCall )
    {
    return !( filterCall.getContext().nextDouble() < fraction );
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof Sample ) )
      return false;
    if( !super.equals( object ) )
      return false;

    Sample sample = (Sample) object;

    if( Double.compare( sample.fraction, fraction ) != 0 )
      return false;
    if( seed != sample.seed )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    long temp;
    result = 31 * result + (int) ( seed ^ seed >>> 32 );
    temp = fraction != +0.0d ? Double.doubleToLongBits( fraction ) : 0L;
    result = 31 * result + (int) ( temp ^ temp >>> 32 );
    return result;
    }
  }