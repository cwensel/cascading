/*
 * Copyright (c) 2007-2023 The Cascading Authors. All Rights Reserved.
 *
 * Project and contact information: https://cascading.wensel.net/
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

package cascading.tap.parquet;

import java.io.IOException;
import java.lang.reflect.Type;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import cascading.flow.FlowProcess;
import cascading.nested.json.JSONCoercibleType;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.CompositeTap;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.PartitionTap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.coerce.Coercions;
import cascading.tuple.type.CoercibleType;
import cascading.tuple.type.DateType;
import cascading.tuple.type.InstantType;
import com.google.common.base.Joiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.mapred.Container;
import org.apache.parquet.hadoop.mapred.DeprecatedParquetInputFormat;
import org.apache.parquet.hadoop.mapred.DeprecatedParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;

import static java.lang.String.format;

/**
 * A Cascading Scheme that converts Parquet groups into Cascading tuples while honoring type information in the
 * fields declarations.
 * <p>
 * If you provide it with sourceFields, it will selectively materialize only the columns for those fields.
 * <p>
 * On read, if any requested fields are not found in the Parquet schema, an error will be thrown to prevent {@code null}
 * values emerging on field name typos.
 * <p>
 * Logical timestamp typed Parquet fields are converted to {@link Instant} values via the {@link InstantType}
 * {@link CoercibleType}. Precision will be honored in either direction.
 * <p>
 * Note, millisecond precision is the default unless microsecond or nanosecond precision is specified.
 * <p>
 * Be default, timestamp values are formatted as ISO-8601 strings. This can be changed by overriding the method
 * {@link #resolveAsInstantType(Type, LogicalTypeAnnotation.TimeUnit)}.
 */
public class TypedParquetScheme extends Scheme<Configuration, RecordReader, OutputCollector, Object[], Object[]>
  {
  public static final Map<Type, PrimitiveType.PrimitiveTypeName> nativeToPrimitive;
  public static final Map<PrimitiveType.PrimitiveTypeName, Type> requiredPrimitiveToNative;
  public static final Map<PrimitiveType.PrimitiveTypeName, Type> optionalPrimitiveToNative;

  static
    {
    Map<Type, PrimitiveType.PrimitiveTypeName> localNativeToPrimitive = new IdentityHashMap<>();
    localNativeToPrimitive.put( Integer.class, PrimitiveType.PrimitiveTypeName.INT32 );
    localNativeToPrimitive.put( Integer.TYPE, PrimitiveType.PrimitiveTypeName.INT32 );
    localNativeToPrimitive.put( Long.class, PrimitiveType.PrimitiveTypeName.INT64 );
    localNativeToPrimitive.put( Long.TYPE, PrimitiveType.PrimitiveTypeName.INT64 );
    localNativeToPrimitive.put( Float.class, PrimitiveType.PrimitiveTypeName.FLOAT );
    localNativeToPrimitive.put( Float.TYPE, PrimitiveType.PrimitiveTypeName.FLOAT );
    localNativeToPrimitive.put( Double.class, PrimitiveType.PrimitiveTypeName.DOUBLE );
    localNativeToPrimitive.put( Double.TYPE, PrimitiveType.PrimitiveTypeName.DOUBLE );
    localNativeToPrimitive.put( Boolean.class, PrimitiveType.PrimitiveTypeName.BOOLEAN );
    localNativeToPrimitive.put( Boolean.TYPE, PrimitiveType.PrimitiveTypeName.BOOLEAN );

    nativeToPrimitive = Collections.unmodifiableMap( localNativeToPrimitive );

    Map<PrimitiveType.PrimitiveTypeName, Type> localRequiredPrimitiveToNative = new IdentityHashMap<>();
    localRequiredPrimitiveToNative.put( PrimitiveType.PrimitiveTypeName.INT32, Integer.TYPE );
    localRequiredPrimitiveToNative.put( PrimitiveType.PrimitiveTypeName.INT64, Long.TYPE );
    localRequiredPrimitiveToNative.put( PrimitiveType.PrimitiveTypeName.FLOAT, Float.TYPE );
    localRequiredPrimitiveToNative.put( PrimitiveType.PrimitiveTypeName.DOUBLE, Double.TYPE );
    localRequiredPrimitiveToNative.put( PrimitiveType.PrimitiveTypeName.BOOLEAN, Boolean.TYPE );

    requiredPrimitiveToNative = Collections.unmodifiableMap( localRequiredPrimitiveToNative );

    Map<PrimitiveType.PrimitiveTypeName, Type> localOptionalPrimitiveToNative = new IdentityHashMap<>();
    localOptionalPrimitiveToNative.put( PrimitiveType.PrimitiveTypeName.INT32, Integer.class );
    localOptionalPrimitiveToNative.put( PrimitiveType.PrimitiveTypeName.INT64, Long.class );
    localOptionalPrimitiveToNative.put( PrimitiveType.PrimitiveTypeName.FLOAT, Float.class );
    localOptionalPrimitiveToNative.put( PrimitiveType.PrimitiveTypeName.DOUBLE, Double.class );
    localOptionalPrimitiveToNative.put( PrimitiveType.PrimitiveTypeName.BOOLEAN, Boolean.class );

    optionalPrimitiveToNative = Collections.unmodifiableMap( localOptionalPrimitiveToNative );
    }

  protected FilterPredicate filterPredicate;
  protected CompressionCodecName compressionCodecName = CompressionCodecName.UNCOMPRESSED;

  protected String parquetSchema;
  protected boolean hasRetrievedFields = false;
  protected JSONCoercibleType jsonCoercibleType = JSONCoercibleType.TYPE;

  public TypedParquetScheme( Fields fields )
    {
    this( fields, fields, null );

    this.parquetSchema = createSchema( fields );
    }

  public TypedParquetScheme( Fields fields, CompressionCodecName compressionCodecName )
    {
    this( fields, fields, null );

    this.parquetSchema = createSchema( fields );

    if( compressionCodecName != null )
      this.compressionCodecName = compressionCodecName;
    }

  public TypedParquetScheme( Fields fields, FilterPredicate filterPredicate )
    {
    this( fields, fields, null );

    this.parquetSchema = createSchema( fields );
    this.filterPredicate = filterPredicate;
    }

  public TypedParquetScheme( Fields fields, FilterPredicate filterPredicate, CompressionCodecName compressionCodecName )
    {
    this( fields, fields, null );

    this.parquetSchema = createSchema( fields );
    this.filterPredicate = filterPredicate;

    if( compressionCodecName != null )
      this.compressionCodecName = compressionCodecName;
    }

  public TypedParquetScheme( Fields sourceFields, Fields sinkFields, final String schema )
    {
    super( sourceFields, sinkFields );
    parquetSchema = schema;
    this.filterPredicate = null;
    }

  public TypedParquetScheme with( JSONCoercibleType jsonCoercibleType )
    {
    this.jsonCoercibleType = jsonCoercibleType;
    return this;
    }

  protected JSONCoercibleType getJSONType()
    {
    return jsonCoercibleType;
    }

  @Override
  public synchronized Fields retrieveSourceFields( FlowProcess<? extends Configuration> flowProcess, Tap tap )
    {
    // we retrieve the fields to capture missing type information
    // note that we merge the 'requested fields' with what we found
    if( hasRetrievedFields )
      return getSourceFields();

    hasRetrievedFields = true;

    if( tap instanceof PartitionTap )
      {
      String[] partitionIdentifiers = getChildPartitionIdentifiers( flowProcess, (PartitionTap) tap );

      if( partitionIdentifiers == null || partitionIdentifiers.length == 0 )
        throw new TapException( "unable to get partition child identifiers, parent resource may not exist: " + tap.getIdentifier() );

      tap = new Hfs( tap.getScheme(), partitionIdentifiers[ 0 ] );
      }
    else if( tap instanceof CompositeTap )
      {
      tap = ( (CompositeTap<?>) tap ).getChildTaps().next();
      }
    else
      {
      // if we are pointing to a directory of partitions, we only need to see the first file to get the header
      // also, internally parquet will create splits etc and assume we are task side thus filtering out relevant metadata
      String[] childIdentifiers = getChildIdentifiers( flowProcess, (Hfs) tap );

      if( childIdentifiers == null || childIdentifiers.length == 0 )
        throw new TapException( "unable to get child identifiers, parent resource may not exist: " + tap.getIdentifier() );

      tap = new Hfs( tap.getScheme(), childIdentifiers[ 0 ] );
      }

    return retrieveSourceFieldsOverride( flowProcess, tap );
    }

  private String[] getChildIdentifiers( FlowProcess<? extends Configuration> flowProcess, Hfs tap )
    {
    try
      {
      return tap.getChildIdentifiers( flowProcess, 10, true );
      }
    catch( IOException exception )
      {
      throw new TapException( "unable to get child identifiers", exception );
      }
    }

  private String[] getChildPartitionIdentifiers( FlowProcess<? extends Configuration> flowProcess, PartitionTap tap )
    {
    try
      {
      return tap.getChildPartitionIdentifiers( flowProcess, true );
      }
    catch( IOException exception )
      {
      throw new TapException( "unable to get child identifiers", exception );
      }
    }

  protected Fields retrieveSourceFieldsOverride( FlowProcess<? extends Configuration> flowProcess, Tap tap )
    {
    MessageType schema = readSchema( flowProcess, tap );

    setSourceFields( createFields( flowProcess, schema, getSourceFields() ) );

    return getSourceFields();
    }

  protected MessageType readSchema( FlowProcess<? extends Configuration> flowProcess, Tap tap )
    {
    try
      {
      List<Footer> footers = getFooters( flowProcess, (Hfs) tap );

      if( footers.isEmpty() )
        throw new TapException( "could not read Parquet metadata at " + ( (Hfs) tap ).getPath() );
      else
        return footers.get( 0 ).getParquetMetadata().getFileMetaData().getSchema();
      }
    catch( IOException e )
      {
      throw new TapException( e );
      }
    }

  protected List<Footer> getFooters( FlowProcess<? extends Configuration> flowProcess, Hfs hfs ) throws IOException
    {
    Configuration jobConf = flowProcess.getConfigCopy();
    DeprecatedParquetInputFormat<?> format = new DeprecatedParquetInputFormat<>();
    FileInputFormat.addInputPath( (JobConf) jobConf, hfs.getPath() );
    return format.getFooters( (JobConf) jobConf );
    }

  protected Fields createFields( FlowProcess<? extends Configuration> flowProcess, MessageType fileSchema, Fields requestedFields )
    {
    if( requestedFields.isUnknown() )
      requestedFields = Fields.ALL;

    for( Comparable<?> requestedField : requestedFields )
      {
      if( !fileSchema.containsField( requestedField.toString() ) )
        throw new TapException( "parquet source scheme does not contain: " + requestedField );
      }

    Fields newFields = Fields.NONE;
    int schemaSize = fileSchema.getFieldCount();

    for( int i = 0; i < schemaSize; i++ )
      {
      org.apache.parquet.schema.Type type = fileSchema.getType( i );
      String fieldName = type.getName();

      PrimitiveType primitiveType = type.asPrimitiveType();
      Repetition repetition = type.getRepetition();

      Type javaType;

      if( repetition == Repetition.REQUIRED )
        javaType = requiredPrimitiveToNative.get( primitiveType.getPrimitiveTypeName() );
      else if( repetition == Repetition.OPTIONAL )
        javaType = optionalPrimitiveToNative.get( primitiveType.getPrimitiveTypeName() );
      else
        throw new IllegalStateException( "unsupported repetition: " + repetition );

      // if the requested fields request a compatible type, honor it
      Type requestedType = requestedFields.hasTypes() && requestedFields.contains( new Fields( fieldName ) ) ? requestedFields.getType( fieldName ) : null;

      LogicalTypeAnnotation logicalTypeAnnotation = type.getLogicalTypeAnnotation();

      // this map should be parameterized and provided by the scheme creator
      if( javaType == null && primitiveType.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.BINARY )
        {
        if( logicalTypeAnnotation.equals( LogicalTypeAnnotation.stringType() ) )
          javaType = String.class;
        else if( logicalTypeAnnotation.equals( LogicalTypeAnnotation.jsonType() ) )
          javaType = getJSONType();
        }
      else if( javaType != null && logicalTypeAnnotation != null )
        {
        if( logicalTypeAnnotation instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation )
          {
          LogicalTypeAnnotation.TimeUnit unit = ( (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) logicalTypeAnnotation ).getUnit();

          if( !( (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) logicalTypeAnnotation ).isAdjustedToUTC() )
            throw new IllegalStateException( "only supports adjusted to UTC timestamp logical types" );

          if( requestedType instanceof InstantType )
            javaType = requestedType;
          else
            javaType = resolveAsInstantType( javaType, unit );
          }
        }

      if( requestedType != null && javaType == null )
        {
        javaType = requestedType;
        }
      else if( requestedType instanceof CoercibleType )
        {
        Class<?> canonicalType = ( (CoercibleType<?>) requestedType ).getCanonicalType();

        // allow swapping of CoercibleTypes if they have effectively the same canonical type
        if( javaType instanceof CoercibleType && Coercions.asNonPrimitive( ( (CoercibleType<?>) javaType ).getCanonicalType() ) == Coercions.asNonPrimitive( canonicalType ) )
          javaType = requestedType;
        else if( Coercions.asNonPrimitive( Coercions.asClass( javaType ) ) == Coercions.asNonPrimitive( canonicalType ) )
          javaType = requestedType;
        else
          throw new IllegalStateException( format( "requested coercible type: %s, with canonical type: %s, does not match parquet declared type: %s", requestedType, canonicalType, type ) );
        }

      if( javaType == null )
        throw new IllegalStateException( "unsupported parquet type: " + type );

      Fields name = new Fields( fieldName, javaType );

      if( requestedFields.contains( name ) )
        newFields = newFields.append( name );
      }

    return newFields;
    }

  /**
   * Resolve the given javaType as an InstantType based on the given unit and javaType.
   *
   * @param javaType
   * @param unit
   * @return Type
   */
  protected Type resolveAsInstantType( Type javaType, LogicalTypeAnnotation.TimeUnit unit )
    {
    switch( unit )
      {
      case MILLIS:
      default:
        return InstantType.ISO_MILLIS;
      case MICROS:
        return InstantType.ISO_MICROS;
      case NANOS:
        return InstantType.ISO_NANOS;
      }
    }

  @Override
  protected void presentSinkFieldsInternal( Fields fields )
    {
    if( getSinkFields().isDefined() )
      {
      // if we haven't declared types, use those presented
      // if we had declared types, the parguet schema will coerce the values
      // we want to honor those declared vs presented
      if( getSinkFields().hasTypes() )
        return;

      fields = getSinkFields().applyTypes( fields );
      }

    setSinkFields( fields );

    parquetSchema = createSchema( fields );
    }

  protected String createSchema( Fields sinkFields )
    {
    if( !sinkFields.isDefined() )
      return null;

    Types.MessageTypeBuilder builder = Types.buildMessage();

    Iterator<Fields> iterator = sinkFields.fieldsIterator();

    Types.GroupBuilder<MessageType> latest = null;

    while( iterator.hasNext() )
      {
      Fields next = iterator.next();

      String name = next.get( 0 ).toString();
      Type fieldType = next.getType( 0 );
      Type type = fieldType;

      if( type == null )
        type = String.class;
      else if( type instanceof CoercibleType )
        type = ( (CoercibleType<?>) type ).getCanonicalType();

      if( type == String.class )
        {
        latest = builder.optional( PrimitiveType.PrimitiveTypeName.BINARY ).as( LogicalTypeAnnotation.stringType() ).named( name );
        }
      else if( nativeToPrimitive.containsKey( type ) )
        { // is a primitive or object base value

        // attempt to honor the type semantics. primitives cannot be null, so are required.
        // that said, a CoercibleType coercion could return nulls even though the canonical type is primitive
        Repetition repetition = ( (Class) type ).isPrimitive() && !( fieldType instanceof CoercibleType ) ? Repetition.REQUIRED : Repetition.OPTIONAL;

        LogicalTypeAnnotation logicalTypeAnnotation = null;

        if( fieldType instanceof DateType )
          {
          String pattern = ( (DateType) fieldType ).getDateFormat().toPattern();
          LogicalTypeAnnotation.TimeUnit unit = LogicalTypeAnnotation.TimeUnit.MILLIS;

          if( pattern.matches( ".*[^S]SSS[^S]+$" ) )
            unit = LogicalTypeAnnotation.TimeUnit.MILLIS;
          else if( pattern.matches( ".*[^S]SSSSSS[^S]+$" ) )
            unit = LogicalTypeAnnotation.TimeUnit.MICROS;
          else if( pattern.matches( ".*[^S]SSSSSSSSS[^S]+$" ) )
            unit = LogicalTypeAnnotation.TimeUnit.NANOS;

          boolean isAdjustedToUTC = true; // DateType always adjusts the value into UTC, or assumes its UTC if no zone given

          logicalTypeAnnotation = LogicalTypeAnnotation.timestampType( isAdjustedToUTC, unit );
          }

        latest = builder.primitive( nativeToPrimitive.get( type ), repetition ).as( logicalTypeAnnotation ).named( name );
        }
      else if( type == Instant.class )
        {
        LogicalTypeAnnotation.TimeUnit unit = LogicalTypeAnnotation.TimeUnit.MILLIS;

        if( fieldType instanceof InstantType )
          {
          TemporalUnit temporalUnit = ( (InstantType) fieldType ).getUnit();

          // millis set above as default
          if( ChronoUnit.MICROS.equals( temporalUnit ) )
            unit = LogicalTypeAnnotation.TimeUnit.MICROS;
          else if( ChronoUnit.NANOS.equals( temporalUnit ) )
            unit = LogicalTypeAnnotation.TimeUnit.NANOS;
          }

        LogicalTypeAnnotation logicalTypeAnnotation = LogicalTypeAnnotation.timestampType( true, unit );
        PrimitiveType.PrimitiveTypeName precision = PrimitiveType.PrimitiveTypeName.INT64;

        latest = builder.primitive( precision, Repetition.OPTIONAL ).as( logicalTypeAnnotation ).named( name );
        }
      else if( fieldType instanceof JSONCoercibleType )
        {
        latest = builder.optional( PrimitiveType.PrimitiveTypeName.BINARY ).as( LogicalTypeAnnotation.jsonType() ).named( name );
        }
      else
        {
        throw new IllegalArgumentException( "unsupported type: " + type.getTypeName() + " on field: " + name );
        }
      }

    if( latest == null )
      throw new IllegalStateException( "unable to generate schema from: " + sinkFields );

    MessageType messageType = latest.named( "tuple" );

    return messageType.toString();
    }

  @Override
  public void sourceConfInit( FlowProcess<? extends Configuration> fp, Tap<Configuration, RecordReader, OutputCollector> tap, Configuration jobConf )
    {
    if( filterPredicate != null )
      ParquetInputFormat.setFilterPredicate( jobConf, filterPredicate );

    ( (JobConf) jobConf ).setInputFormat( DeprecatedParquetInputFormat.class );
    // this implementation of read support will fail if a requested fields is not found in the schema
    // use the base class TupleReadSupport for the original behavior
    ParquetInputFormat.setReadSupportClass( (JobConf) jobConf, TypedTupleReadSupport.class );
    Fields sourceFields = getSourceFields();

    String fieldsString = Joiner.on( ':' ).join( sourceFields.iterator() );
    jobConf.set( "parquet.cascading.requested.fields", fieldsString );
    }

  @Override
  public void sourcePrepare( FlowProcess<? extends Configuration> flowProcess, SourceCall<Object[], RecordReader> sourceCall )
    {
    Object value = sourceCall.getInput().createValue();
    BiConsumer<TupleEntry, Tuple> setter = sourceCall.getIncomingEntry().getFields().isUnknown() ? TupleEntry::setTuple : TupleEntry::setCanonicalTuple;
    sourceCall.setContext( new Object[]{value, setter} );
    }

  @Override
  public void sourceRePrepare( FlowProcess<? extends Configuration> flowProcess, SourceCall<Object[], RecordReader> sourceCall )
    {
    Object value = sourceCall.getInput().createValue();
    sourceCall.getContext()[ 0 ] = value;
    }

  public boolean source( FlowProcess<? extends Configuration> flowProcess, SourceCall<Object[], RecordReader> sourceCall ) throws IOException
    {
    Container<Tuple> value = (Container<Tuple>) sourceCall.getContext()[ 0 ];
    BiConsumer<TupleEntry, Tuple> setter = (BiConsumer<TupleEntry, Tuple>) sourceCall.getContext()[ 1 ];

    while( true )
      {
      boolean hasNext = sourceCall.getInput().next( null, value );

      // value may be null and hasNext = false, unsure why this happens but is intentional
      // sourceRePrepare catches this case and will construct a new value container on the next
      // partition iteration
      // previously it was thought ParquetInputFormat.TASK_SIDE_METADATA needed to be set to force
      // footers to read metadata, this only used more memory and slowed processing
      if( !hasNext )
        return false;

      // unsure when value == null and hasNext is true, but we should force a new value object
      // and continue until eof
      if( value == null )
        {
        value = (Container<Tuple>) sourceCall.getInput().createValue();
        sourceCall.getContext()[ 0 ] = value;
        continue;
        }

      Tuple tuple = value.get();

      setter.accept( sourceCall.getIncomingEntry(), tuple );

      return true;
      }
    }

  @Override
  public boolean isSink()
    {
    return true;
    }

  @Override
  public void sinkConfInit( FlowProcess<? extends Configuration> fp, Tap<Configuration, RecordReader, OutputCollector> tap, Configuration jobConf )
    {
    if( jobConf.get( "mapreduce.task.partition" ) == null )
      jobConf.setInt( "mapreduce.task.partition", 0 );

    jobConf.set( ParquetOutputFormat.COMPRESSION, compressionCodecName.name() );
    DeprecatedParquetOutputFormat.setAsOutputFormat( (JobConf) jobConf );
    jobConf.set( TupleWriteSupport.PARQUET_CASCADING_SCHEMA, parquetSchema );
    ParquetOutputFormat.setWriteSupportClass( (JobConf) jobConf, TypedTupleWriteSupport.class );
    }

  @Override
  public void sink( FlowProcess<? extends Configuration> fp, SinkCall<Object[], OutputCollector> sink )
    throws IOException
    {
    TupleEntry tuple = sink.getOutgoingEntry();
    OutputCollector outputCollector = sink.getOutput();
    outputCollector.collect( null, tuple );
    }
  }
