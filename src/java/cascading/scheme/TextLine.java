/*
 * Copyright (c) 2007-2008 Vinculum Technologies, Inc. All Rights Reserved.
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

package cascading.scheme;

import java.io.IOException;

import cascading.tap.hadoop.ZipInputFormat;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * A TextLine is a type of {@link Scheme} for plain text files. Files are broken into
 * lines. Either line-feed or carriage-return are used to signal end of line. Keys are the
 * position in the file, and values are the line of text.
 * <p/>
 * If all the input files end with ".zip", the {@link ZipInputFormat} will be used. This is not
 * bi-directional, so zip files cannot be written.
 */
public class TextLine extends Scheme
  {
  /** Field serialVersionUID */
  private static final long serialVersionUID = 1L;
  /** Field DEFAULT_SOURCE_FIELDS */
  public static final Fields DEFAULT_SOURCE_FIELDS = new Fields( "offset", "line" );

  /**
   * Creates a new TextLine instance that sources "offset" and "line" fields, and sinks all incoming fields, where
   * "offset" is the byte offset in the input file. By default, numSinkParts is set to 1.
   */
  public TextLine()
    {
    super( DEFAULT_SOURCE_FIELDS, 1 );
    }

  /**
   * Creates a new TextLine instance that sources "offset" and "line" fields, and sinks all incoming fields, where
   * "offset" is the byte offset in the input file.
   *
   * @param numSinkFileParts of type int
   */
  public TextLine( int numSinkFileParts )
    {
    super( DEFAULT_SOURCE_FIELDS, numSinkFileParts );
    }

  /**
   * Creates a new TextLine instance.
   *
   * @param sourceFields the source fields for this scheme
   * @param sinkFields   the sink fields for this scheme
   */
  public TextLine( Fields sourceFields, Fields sinkFields )
    {
    super( sourceFields, sinkFields );

    if( sourceFields.size() != 2 )
      throw new IllegalArgumentException( "this scheme requires only two fields, given [" + sourceFields + "]" );
    }

  /**
   * Constructor
   *
   * @param sourceFields the source fields for this scheme
   */
  public TextLine( Fields sourceFields )
    {
    super( sourceFields );

    if( sourceFields.size() != 2 )
      throw new IllegalArgumentException( "this scheme requires only two fields, given [" + sourceFields + "]" );
    }

  @Override
  public void sourceInit( JobConf conf )
    {
    if( hasZippedFiles( conf.getInputPaths() ) )
      conf.setInputFormat( ZipInputFormat.class );
    else
      conf.setInputFormat( TextInputFormat.class );
    }

  private boolean hasZippedFiles( Path[] paths )
    {
    boolean isZipped = paths[ 0 ].getName().endsWith( ".zip" );

    for( int i = 1; i < paths.length; i++ )
      {
      if( isZipped != paths[ i ].getName().endsWith( ".zip" ) )
        throw new IllegalStateException( "cannot mix zipped and upzippled files" );
      }

    return isZipped;
    }

  @Override
  public void sinkInit( JobConf conf )
    {
    if( conf.getOutputPath().getName().endsWith( ".zip" ) )
      throw new IllegalStateException( "cannot write zip files: " + conf.getOutputPath() );

    conf.setOutputFormat( TextOutputFormat.class );
    }

  @Override
  public Tuple source( WritableComparable key, Writable value )
    {
    Tuple tuple = new Tuple();
    int[] currPos = sourceFields.getPos();

    for( int currPo : currPos )
      {
      if( currPo == 0 )
        tuple.add( key.toString() );

      if( currPo == 1 )
        tuple.add( value.toString() );
      }

    return tuple;
    }

  @Override
  public InputFormat getInputFormat( JobConf conf )
    {
    if( hasZippedFiles( conf.getInputPaths() ) )
      return new ZipInputFormat();
    else
      return new TextInputFormat();
    }

  public OutputFormat getOutputFormat( JobConf conf )
    {
    return new TextOutputFormat();
    }

  @Override
  public void sink( Fields inFields, Tuple tuple, OutputCollector outputCollector ) throws IOException
    {
    outputCollector.collect( null, tuple.get( inFields, sinkFields ) );
    }

  }
