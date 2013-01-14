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

package cascading.scheme.hadoop;

import java.beans.ConstructorProperties;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * A TextLine is a type of {@link cascading.scheme.Scheme} for plain text files. Files are broken into
 * lines. Either line-feed or carriage-return are used to signal end of line.
 * <p/>
 * By default, this scheme returns a {@link Tuple} with two fields, "offset" and "line".
 * <p/>
 * Many of the constructors take both "sourceFields" and "sinkFields". sourceFields denote the field names
 * to be used instead of the names "offset" and "line". sinkFields is a selector and is by default {@link Fields#ALL}.
 * Any available field names can be given if only a subset of the incoming fields should be used.
 * <p/>
 * If a {@link Fields} instance is passed on the constructor as sourceFields having only one field, the return tuples
 * will simply be the "line" value using the given field name.
 * <p/>
 * Note that TextLine will concatenate all the Tuple values for the selected fields with a TAB delimiter before
 * writing out the line.
 * <p/>
 * Note sink compression is {@link Compress#DISABLE} by default. If {@code null} is passed to the constructor
 * for the compression value, it will remain disabled.
 * <p/>
 * If any of the input files end with ".zip", an error will be thrown.
 * * <p/>
 * By default, all text is encoded/decoded as UTF-8. This can be changed via the {@code charsetName} constructor
 * argument.
 */
public class TextLine extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]>
  {
  public enum Compress
    {
      DEFAULT, ENABLE, DISABLE
    }

  public static final String DEFAULT_CHARSET = "UTF-8";

  /** Field serialVersionUID */
  private static final long serialVersionUID = 1L;
  /** Field DEFAULT_SOURCE_FIELDS */
  public static final Fields DEFAULT_SOURCE_FIELDS = new Fields( "offset", "line" );

  /** Field sinkCompression */
  Compress sinkCompression = Compress.DISABLE;

  String charsetName = DEFAULT_CHARSET;

  /**
   * Creates a new TextLine instance that sources "offset" and "line" fields, and sinks all incoming fields, where
   * "offset" is the byte offset in the input file.
   */
  public TextLine()
    {
    super( DEFAULT_SOURCE_FIELDS );
    }

  /**
   * Creates a new TextLine instance that sources "offset" and "line" fields, and sinks all incoming fields, where
   * "offset" is the byte offset in the input file.
   *
   * @param numSinkParts of type int
   */
  @ConstructorProperties({"numSinkParts"})
  public TextLine( int numSinkParts )
    {
    super( DEFAULT_SOURCE_FIELDS, numSinkParts );
    }

  /**
   * Creates a new TextLine instance that sources "offset" and "line" fields, and sinks all incoming fields, where
   * "offset" is the byte offset in the input file.
   *
   * @param sinkCompression of type Compress
   */
  @ConstructorProperties({"sinkCompression"})
  public TextLine( Compress sinkCompression )
    {
    super( DEFAULT_SOURCE_FIELDS );

    setSinkCompression( sinkCompression );
    }

  /**
   * Creates a new TextLine instance. If sourceFields has one field, only the text line will be returned in the
   * subsequent tuples.
   *
   * @param sourceFields the source fields for this scheme
   * @param sinkFields   the sink fields for this scheme
   */
  @ConstructorProperties({"sourceFields", "sinkFields"})
  public TextLine( Fields sourceFields, Fields sinkFields )
    {
    super( sourceFields, sinkFields );

    verify( sourceFields );
    }

  /**
   * Creates a new TextLine instance. If sourceFields has one field, only the text line will be returned in the
   * subsequent tuples.
   *
   * @param sourceFields the source fields for this scheme
   * @param sinkFields   the sink fields for this scheme
   * @param charsetName  of type String
   */
  @ConstructorProperties({"sourceFields", "sinkFields", "charsetName"})
  public TextLine( Fields sourceFields, Fields sinkFields, String charsetName )
    {
    super( sourceFields, sinkFields );

    // throws an exception if not found
    setCharsetName( charsetName );

    verify( sourceFields );
    }

  /**
   * Creates a new TextLine instance. If sourceFields has one field, only the text line will be returned in the
   * subsequent tuples.
   *
   * @param sourceFields the source fields for this scheme
   * @param sinkFields   the sink fields for this scheme
   * @param numSinkParts of type int
   */
  @ConstructorProperties({"sourceFields", "sinkFields", "numSinkParts"})
  public TextLine( Fields sourceFields, Fields sinkFields, int numSinkParts )
    {
    super( sourceFields, sinkFields, numSinkParts );

    verify( sourceFields );
    }

  /**
   * Constructor TextLine creates a new TextLine instance. If sourceFields has one field, only the text line will be returned in the
   * subsequent tuples.
   *
   * @param sourceFields    of type Fields
   * @param sinkFields      of type Fields
   * @param sinkCompression of type Compress
   */
  @ConstructorProperties({"sourceFields", "sinkFields", "sinkCompression"})
  public TextLine( Fields sourceFields, Fields sinkFields, Compress sinkCompression )
    {
    super( sourceFields, sinkFields );

    setSinkCompression( sinkCompression );

    verify( sourceFields );
    }

  /**
   * Constructor TextLine creates a new TextLine instance. If sourceFields has one field, only the text line will be returned in the
   * subsequent tuples.
   *
   * @param sourceFields    of type Fields
   * @param sinkFields      of type Fields
   * @param sinkCompression of type Compress
   * @param charsetName     of type String
   */
  @ConstructorProperties({"sourceFields", "sinkFields", "sinkCompression", "charsetName"})
  public TextLine( Fields sourceFields, Fields sinkFields, Compress sinkCompression, String charsetName )
    {
    super( sourceFields, sinkFields );

    setSinkCompression( sinkCompression );

    // throws an exception if not found
    setCharsetName( charsetName );

    verify( sourceFields );
    }

  /**
   * Constructor TextLine creates a new TextLine instance. If sourceFields has one field, only the text line will be returned in the
   * subsequent tuples.
   *
   * @param sourceFields    of type Fields
   * @param sinkFields      of type Fields
   * @param sinkCompression of type Compress
   * @param numSinkParts    of type int
   */
  @ConstructorProperties({"sourceFields", "sinkFields", "sinkCompression", "numSinkParts"})
  public TextLine( Fields sourceFields, Fields sinkFields, Compress sinkCompression, int numSinkParts )
    {
    super( sourceFields, sinkFields, numSinkParts );

    setSinkCompression( sinkCompression );

    verify( sourceFields );
    }

  /**
   * Constructor TextLine creates a new TextLine instance. If sourceFields has one field, only the text line will be returned in the
   * subsequent tuples.
   *
   * @param sourceFields    of type Fields
   * @param sinkFields      of type Fields
   * @param sinkCompression of type Compress
   * @param numSinkParts    of type int
   * @param charsetName     of type String
   */
  @ConstructorProperties({"sourceFields", "sinkFields", "sinkCompression", "numSinkParts", "charsetName"})
  public TextLine( Fields sourceFields, Fields sinkFields, Compress sinkCompression, int numSinkParts, String charsetName )
    {
    super( sourceFields, sinkFields, numSinkParts );

    setSinkCompression( sinkCompression );

    // throws an exception if not found
    setCharsetName( charsetName );

    verify( sourceFields );
    }

  /**
   * Creates a new TextLine instance. If sourceFields has one field, only the text line will be returned in the
   * subsequent tuples.
   *
   * @param sourceFields the source fields for this scheme
   */
  @ConstructorProperties({"sourceFields"})
  public TextLine( Fields sourceFields )
    {
    super( sourceFields );

    verify( sourceFields );
    }

  /**
   * Creates a new TextLine instance. If sourceFields has one field, only the text line will be returned in the
   * subsequent tuples.
   *
   * @param sourceFields the source fields for this scheme
   * @param charsetName  of type String
   */
  @ConstructorProperties({"sourceFields", "charsetName"})
  public TextLine( Fields sourceFields, String charsetName )
    {
    super( sourceFields );

    // throws an exception if not found
    setCharsetName( charsetName );

    verify( sourceFields );
    }

  /**
   * Creates a new TextLine instance. If sourceFields has one field, only the text line will be returned in the
   * subsequent tuples. The resulting data set will have numSinkParts.
   *
   * @param sourceFields the source fields for this scheme
   * @param numSinkParts of type int
   */
  @ConstructorProperties({"sourceFields", "numSinkParts"})
  public TextLine( Fields sourceFields, int numSinkParts )
    {
    super( sourceFields, numSinkParts );

    verify( sourceFields );
    }

  protected void setCharsetName( String charsetName )
    {
    if( charsetName != null )
      this.charsetName = charsetName;

    Charset.forName( this.charsetName );
    }

  protected void verify( Fields sourceFields )
    {
    if( sourceFields.size() < 1 || sourceFields.size() > 2 )
      throw new IllegalArgumentException( "this scheme requires either one or two source fields, given [" + sourceFields + "]" );
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
   * Method setSinkCompression sets the sinkCompression of this TextLine object. If null, compression will remain disabled.
   *
   * @param sinkCompression the sinkCompression of this TextLine object.
   */
  public void setSinkCompression( Compress sinkCompression )
    {
    if( sinkCompression != null ) // leave disabled if null
      this.sinkCompression = sinkCompression;
    }

  @Override
  public void sourceConfInit( FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf )
    {
    if( hasZippedFiles( FileInputFormat.getInputPaths( conf ) ) )
      throw new IllegalStateException( "cannot read zip files: " + Arrays.toString( FileInputFormat.getInputPaths( conf ) ) );

    conf.setInputFormat( TextInputFormat.class );
    }

  private boolean hasZippedFiles( Path[] paths )
    {
    boolean isZipped = paths[ 0 ].getName().endsWith( ".zip" );

    for( int i = 1; i < paths.length; i++ )
      {
      if( isZipped != paths[ i ].getName().endsWith( ".zip" ) )
        throw new IllegalStateException( "cannot mix zipped and upzipped files" );
      }

    return isZipped;
    }

  @Override
  public void presentSourceFields( FlowProcess<JobConf> flowProcess, Tap tap, Fields fields )
    {
    // do nothing to change TextLine state
    }

  @Override
  public void presentSinkFields( FlowProcess<JobConf> flowProcess, Tap tap, Fields fields )
    {
    // do nothing to change TextLine state
    }

  @Override
  public void sinkConfInit( FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf )
    {
    if( tap.getFullIdentifier( conf ).endsWith( ".zip" ) )
      throw new IllegalStateException( "cannot write zip files: " + FileOutputFormat.getOutputPath( conf ) );

    if( getSinkCompression() == Compress.DISABLE )
      conf.setBoolean( "mapred.output.compress", false );
    else if( getSinkCompression() == Compress.ENABLE )
      conf.setBoolean( "mapred.output.compress", true );

    conf.setOutputKeyClass( Text.class ); // be explicit
    conf.setOutputValueClass( Text.class ); // be explicit
    conf.setOutputFormat( TextOutputFormat.class );
    }

  @Override
  public void sourcePrepare( FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall )
    {
    if( sourceCall.getContext() == null )
      sourceCall.setContext( new Object[ 3 ] );

    sourceCall.getContext()[ 0 ] = sourceCall.getInput().createKey();
    sourceCall.getContext()[ 1 ] = sourceCall.getInput().createValue();
    sourceCall.getContext()[ 2 ] = Charset.forName( charsetName );
    }

  @Override
  public boolean source( FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall ) throws IOException
    {
    if( !sourceReadInput( sourceCall ) )
      return false;

    sourceHandleInput( sourceCall );

    return true;
    }

  private boolean sourceReadInput( SourceCall<Object[], RecordReader> sourceCall ) throws IOException
    {
    Object[] context = sourceCall.getContext();

    return sourceCall.getInput().next( context[ 0 ], context[ 1 ] );
    }

  protected void sourceHandleInput( SourceCall<Object[], RecordReader> sourceCall )
    {
    TupleEntry result = sourceCall.getIncomingEntry();

    int index = 0;
    Object[] context = sourceCall.getContext();

    // coerce into canonical forms
    if( getSourceFields().size() == 2 )
      result.setLong( index++, ( (LongWritable) context[ 0 ] ).get() );

    result.setString( index, makeEncodedString( context ) );
    }

  protected String makeEncodedString( Object[] context )
    {
    Text text = (Text) context[ 1 ];
    return new String( text.getBytes(), 0, text.getLength(), (Charset) context[ 2 ] );
    }

  @Override
  public void sourceCleanup( FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall )
    {
    sourceCall.setContext( null );
    }

  @Override
  public void sinkPrepare( FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall ) throws IOException
    {
    sinkCall.setContext( new Object[ 2 ] );

    sinkCall.getContext()[ 0 ] = new Text();
    sinkCall.getContext()[ 1 ] = Charset.forName( charsetName );
    }

  @Override
  public void sink( FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall ) throws IOException
    {
    Text text = (Text) sinkCall.getContext()[ 0 ];
    Charset charset = (Charset) sinkCall.getContext()[ 1 ];
    String line = sinkCall.getOutgoingEntry().getTuple().toString();

    text.set( line.getBytes( charset ) );

    // it's ok to use NULL here so the collector does not write anything
    sinkCall.getOutput().collect( null, text );
    }
  }
