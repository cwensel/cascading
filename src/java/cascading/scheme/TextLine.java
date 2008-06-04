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

package cascading.scheme;

import java.io.IOException;

import cascading.tap.Tap;
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
 * lines. Either line-feed or carriage-return are used to signal end of line.
 * <p/>
 * By default, this scheme returns a {@link Tuple} with two fields, "offset" and "line". But if
 * a {@link Fields} is passed on the constructor with one field, the return tuples will simply
 * be the "line" value. *
 * <p/>
 * If all the input files end with ".zip", the {@link ZipInputFormat} will be used. This is not
 * bi-directional, so zip files cannot be written.
 */
public class TextLine extends Scheme
  {
  enum Compress
    {
      DEFAULT, ENABLE, DISABLE
    }

  /** Field serialVersionUID */
  private static final long serialVersionUID = 1L;
  /** Field DEFAULT_SOURCE_FIELDS */
  public static final Fields DEFAULT_SOURCE_FIELDS = new Fields( "offset", "line" );

  /** Field sinkCompression */
  Compress sinkCompression = Compress.DISABLE;

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
   * Creates a new TextLine instance. If sourceFields has one field, only the text line will be returned in the
   * subsequent tuples.
   *
   * @param sourceFields the source fields for this scheme
   * @param sinkFields   the sink fields for this scheme
   */
  public TextLine( Fields sourceFields, Fields sinkFields )
    {
    super( sourceFields, sinkFields );

    if( sourceFields.size() < 1 || sourceFields.size() > 2 )
      throw new IllegalArgumentException( "this scheme requires either one or two fields, given [" + sourceFields + "]" );
    }

  /**
   * Creates a new TextLine instance. If sourceFields has one field, only the text line will be returned in the
   * subsequent tuples.
   *
   * @param sourceFields the source fields for this scheme
   */
  public TextLine( Fields sourceFields )
    {
    super( sourceFields );

    if( sourceFields.size() < 1 || sourceFields.size() > 2 )
      throw new IllegalArgumentException( "this scheme requires either one or two fields, given [" + sourceFields + "]" );
    }

  /**
   * Method getSinkCompression returns the sinkCompression of this TextLine object.
   *
   * @return the sinkCompression (type Compress) of this TextLine object.
   */
  public Compress getSinkCompression()
    {
    return sinkCompression;
    }

  /**
   * Method setSinkCompression sets the sinkCompression of this TextLine object.
   *
   * @param sinkCompression the sinkCompression of this TextLine object.
   */
  public void setSinkCompression( Compress sinkCompression )
    {
    this.sinkCompression = sinkCompression;
    }

  @Override
  public void sourceInit( Tap tap, JobConf conf )
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
  public void sinkInit( Tap tap, JobConf conf ) throws IOException
    {
    if( tap.getQualifiedPath( conf ).toString().endsWith( ".zip" ) )
      throw new IllegalStateException( "cannot write zip files: " + conf.getOutputPath() );

    if( getSinkCompression() == Compress.DISABLE )
      conf.setBoolean( "mapred.output.compress", false );
    else if( getSinkCompression() == Compress.ENABLE )
      conf.setBoolean( "mapred.output.compress", true );

    conf.setOutputFormat( TextOutputFormat.class );
    }

  @Override
  public InputFormat getInputFormat( JobConf conf )
    {
    if( hasZippedFiles( conf.getInputPaths() ) )
      return new ZipInputFormat();
    else
      return new TextInputFormat();
    }

  @Override
  public OutputFormat getOutputFormat( JobConf conf )
    {
    return new TextOutputFormat();
    }

  @Override
  public Tuple source( WritableComparable key, Writable value )
    {
    Tuple tuple = new Tuple();

    if( sourceFields.size() == 2 )
      tuple.add( key.toString() );

    tuple.add( value.toString() );

    return tuple;
    }

  @Override
  public void sink( Fields inFields, Tuple tuple, OutputCollector outputCollector ) throws IOException
    {
    outputCollector.collect( null, tuple.get( inFields, sinkFields ) );
    }

  }
