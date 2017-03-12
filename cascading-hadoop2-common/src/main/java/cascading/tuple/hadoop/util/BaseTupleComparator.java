/*
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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

package cascading.tuple.hadoop.util;

import java.io.IOException;

import cascading.CascadingException;
import cascading.tuple.Tuple;
import cascading.tuple.hadoop.TupleSerializationProps;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public abstract class BaseTupleComparator extends DeserializerComparator<Tuple> implements Configurable
  {
  private static final Logger LOG = LoggerFactory.getLogger( BaseTupleComparator.class );

  RawComparison actual;

  @Override
  protected boolean canPerformRawComparisons()
    {
    return !getConf().getBoolean( TupleSerializationProps.SERIALIZATION_COMPARISON_BITWISE_PREVENT, false );
    }

  @Override
  public void setConf( Configuration conf )
    {
    super.setConf( conf );

    if( conf == null )
      return;

    if( performRawComparison() )
      LOG.info( "enabling raw byte comparison and ordering" );

    if( performRawComparison() )
      actual = getByteComparison();
    else
      actual = getStreamComparison();
    }

  protected RawComparison getStreamComparison()
    {
    return new StreamComparison();
    }

  protected abstract RawComparison getByteComparison();

  public int compare( byte[] b1, int s1, int l1, byte[] b2, int s2, int l2 )
    {
    return actual.compare( b1, s1, l1, b2, s2, l2 );
    }

  public int compare( Tuple lhs, Tuple rhs )
    {
    return compareTuples( groupComparators, lhs, rhs );
    }

  protected interface RawComparison
    {
    int compare( byte[] b1, int s1, int l1, byte[] b2, int s2, int l2 );
    }

  class StreamComparison implements RawComparison
    {
    @Override
    public int compare( byte[] b1, int s1, int l1, byte[] b2, int s2, int l2 )
      {
      try
        {
        lhsBuffer.reset( b1, s1, l1 );
        rhsBuffer.reset( b2, s2, l2 );

        return compareTuples( keyTypes, groupComparators );
        }
      catch( IOException exception )
        {
        throw new CascadingException( exception );
        }
      finally
        {
        lhsBuffer.clear();
        rhsBuffer.clear();
        }
      }
    }
  }
