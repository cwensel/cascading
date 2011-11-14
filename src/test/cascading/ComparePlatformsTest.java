/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

package cascading;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.PlatformTestCase.getTestRoot;

/**
 *
 */
public class ComparePlatformsTest extends CascadingTestCase
  {
  private static final Logger LOG = LoggerFactory.getLogger( ComparePlatformsTest.class );

  public static Test suite() throws Exception
    {
    String root = getTestRoot();
    File localRoot = new File( root + "/local" );
    File hadoopRoot = new File( root + "/hadoop" );

    LinkedList<File> localFiles = new LinkedList<File>( FileUtils.listFiles( localRoot, new RegexFileFilter( "^[\\w-]+" ), TrueFileFilter.INSTANCE ) );
    LinkedList<File> hadoopFiles = new LinkedList<File>();

    int rootLength = localRoot.toString().length() + 1;

    ListIterator<File> iterator = localFiles.listIterator();
    while( iterator.hasNext() )
      {
      File localFile = iterator.next();
      File file = new File( hadoopRoot, localFile.toString().substring( rootLength ) );

      if( localFile.toString().endsWith( "-nondeterministic" ) )
        iterator.remove();
      else if( file.exists() )
        hadoopFiles.add( file );
      else
        iterator.remove();
      }

    LOG.info( "running {} comparisons", localFiles.size() );

    TestSuite suite = new TestSuite();

    for( int i = 0; i < localFiles.size(); i++ )
      {
      File localFile = localFiles.get( i );
      File hadoopFile = hadoopFiles.get( i );

      suite.addTest( new CompareTestCase( localFile, hadoopFile ) );
      }

    return suite;
    }

  public static class CompareTestCase extends TestCase
    {
    File localFile;
    File hadoopFile;

    public CompareTestCase( File localFile, File hadoopFile )
      {
      super( "testFiles" );
      this.localFile = localFile;
      this.hadoopFile = hadoopFile;
      }

    public void testFiles() throws IOException
      {
      LinkedList<String> localLines = getLines( localFile );
      LinkedList<String> hadoopLines = getLines( hadoopFile );

      assertEquals( localFile + " != " + hadoopFile, localLines.size(), hadoopLines.size() );

      if( localLines.size() == 0 )
        return;

      Collections.sort( localLines );
      Collections.sort( hadoopLines );

      if( hasLineNumbers( localLines ) )
        {
        trimLineNumbers( localLines );
        trimLineNumbers( hadoopLines );
        }

      for( int i = 0; i < localLines.size(); i++ )
        {
        String localLine = localLines.get( i );

        assertTrue( localFile + " - not in hadoop lines: " + localLine, hadoopLines.contains( localLine ) );
        }
      }

    private void trimLineNumbers( LinkedList<String> lines )
      {
      ListIterator<String> iterator = lines.listIterator();

      while( iterator.hasNext() )
        iterator.set( iterator.next().replaceFirst( "^\\d+\\s(.*)$", "$1" ) );
      }

    private boolean hasLineNumbers( List<String> lines )
      {
      List<Integer> values = new LinkedList<Integer>();

      for( String line : lines )
        {
        if( !line.matches( "^\\d+\\s.*$" ) )
          return false;

        String value = line.replaceFirst( "^(\\d+)\\s.*$", "$1" );

        if( value == null || value.isEmpty() )
          return false;

        values.add( Integer.parseInt( value ) );
        }

      Collections.sort( values );

      int last = -1;
      for( Integer value : values )
        {
        if( last >= value )
          return false;

        last = value;
        }

      return true;
      }

    private LinkedList<String> getLines( File localFile ) throws IOException
      {
      LinkedList<String> lines = new LinkedList<String>();

      if( !localFile.isDirectory() )
        return populate( localFile, lines );

      Collection<File> subFiles = FileUtils.listFiles( localFile, new RegexFileFilter( "^part-.*" ), null );

      for( File subFile : subFiles )
        populate( subFile, lines );

      return lines;
      }

    private LinkedList<String> populate( File localFile, LinkedList<String> lines ) throws IOException
      {
      LineIterator iterator = FileUtils.lineIterator( localFile, "UTF-8" );

      while( iterator.hasNext() )
        lines.add( iterator.next() );

      return lines;
      }
    }
  }