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

package cascading.tap.hadoop;

import java.io.IOException;

import cascading.CascadingTestCase;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.partition.DelimitedPartition;
import cascading.tuple.Fields;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

/**
 *
 */
public class PartitionTapTest extends CascadingTestCase
  {
  @Test
  public void testPartitionTap() throws IOException
    {
    String comment = "improved";
    long ts = System.currentTimeMillis();
    getDuration( 100 ); // warmup

    int runs = 4;

    int[] sizes = new int[]
      {
        1, 10, 100, 1000, 10000, 100000, 1000000
      };

    long[][] durations = new long[ sizes.length ][ runs ];

    for( int run = 0; run < runs; run++ )
      {
      for( int i = 0; i < sizes.length; i++ )
        durations[ i ][ run ] = getDuration( sizes[ i ] );
      }

    for( int i = 0; i < durations.length; i++ )
      {
      long[] duration = durations[ i ];
      DescriptiveStatistics stats = new DescriptiveStatistics();

      for( long value : duration )
        stats.addValue( value );

      String string = String.format( "%s,%d,%d,%f\n", comment, ts, sizes[ i ], stats.getMean() );
      System.out.print( string );
//      Files.write( Paths.get( "partition-tap.csv" ), string.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND );
      }
    }

  public long getDuration( int size )
    {
    long start = System.currentTimeMillis();
    createPartitionTap( size ).sourceConfInit( new HadoopFlowProcess( new JobConf() ), new JobConf() );
    return System.currentTimeMillis() - start;
    }

  protected PartitionTap createPartitionTap( int size )
    {
    final String path = "some/path/that/is/particularly/long/and/unwieldy/";
    final String[] values = new String[ size ];

    for( int i = 0; i < values.length; i++ )
      values[ i ] = path + i;

    return new PartitionTap( new Hfs( new TextDelimited( new Fields( "foo" ), "," ), path ), new DelimitedPartition( new Fields( "foo" ) ) )
      {
      @Override
      public String[] getChildPartitionIdentifiers( FlowProcess<? extends Configuration> flowProcess, boolean fullyQualified ) throws IOException
        {
        return values;
        }
      };
    }
  }
