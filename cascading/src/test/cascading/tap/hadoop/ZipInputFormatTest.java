/*
 * Copyright (c) 2007-2008 Concurrent, Inc. All Rights Reserved.
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

package cascading.tap.hadoop;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.BitSet;
import java.util.Random;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import cascading.CascadingTestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.InMemoryFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class ZipInputFormatTest extends CascadingTestCase
  {
  private static final Log LOG = LogFactory.getLog( ZipInputFormatTest.class.getName() );

  private static int MAX_LENGTH = 10000;
  private static int MAX_ENTRIES = 100;

  private static Path workDir = new Path( "build/test/output/ziptest" );

  public void testSplits() throws Exception
    {
    JobConf job = new JobConf();
    FileSystem currentFs = FileSystem.get( job );

    Path file = new Path( workDir, "test.zip" );

    Reporter reporter = Reporter.NULL;

    int seed = new Random().nextInt();
    LOG.info( "seed = " + seed );
    Random random = new Random( seed );
    job.setInputPath( file );

    for( int entries = 1; entries < MAX_ENTRIES; entries += random.nextInt( MAX_ENTRIES / 10 ) + 1 )
      {
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      ZipOutputStream zos = new ZipOutputStream( byteArrayOutputStream );
      long length = 0;

      LOG.debug( "creating; zip file with entries = " + entries );

      // for each entry in the zip file
      for( int entryCounter = 0; entryCounter < entries; entryCounter++ )
        {
        // construct zip entries splitting MAX_LENGTH between entries
        long entryLength = MAX_LENGTH / entries;
        ZipEntry zipEntry = new ZipEntry( "/entry" + entryCounter + ".txt" );
        zipEntry.setMethod( ZipEntry.DEFLATED );
        zos.putNextEntry( zipEntry );

        for( length = entryCounter * entryLength; length < ( entryCounter + 1 ) * entryLength; length++ )
          {
          zos.write( Long.toString( length ).getBytes() );
          zos.write( "\n".getBytes() );
          }

        zos.flush();
        zos.closeEntry();
        }

      zos.flush();
      zos.close();

      currentFs.delete( file );

      if( currentFs instanceof InMemoryFileSystem )
        ( (InMemoryFileSystem) currentFs ).reserveSpaceWithCheckSum( file, byteArrayOutputStream.size() );

      OutputStream outputStream = currentFs.create( file );

      byteArrayOutputStream.writeTo( outputStream );
      outputStream.close();

      ZipInputFormat format = new ZipInputFormat();
      format.configure( job );
      LongWritable key = new LongWritable();
      Text value = new Text();
      InputSplit[] splits = format.getSplits( job, 100 );

      BitSet bits = new BitSet( (int) length );
      for( int j = 0; j < splits.length; j++ )
        {
        LOG.debug( "split[" + j + "]= " + splits[ j ] );
        RecordReader<LongWritable, Text> reader = format.getRecordReader( splits[ j ], job, reporter );

        try
          {
          int count = 0;

          while( reader.next( key, value ) )
            {
            int v = Integer.parseInt( value.toString() );
            LOG.debug( "read " + v );

            if( bits.get( v ) )
              LOG.warn( "conflict with " + v + " in split " + j + " at position " + reader.getPos() );

            assertFalse( "key in multiple partitions.", bits.get( v ) );
            bits.set( v );
            count++;
            }

          LOG.debug( "splits[" + j + "]=" + splits[ j ] + " count=" + count );
          }
        finally
          {
          reader.close();
          }
        }

      assertEquals( "some keys in no partition.", length, bits.cardinality() );
      }
    }

  }