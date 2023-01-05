package cascading.tap.parquet.checked;

public class CheckedParquetSchema extends ParquetTupleScheme{
    private static final Logger LOG = LoggerFactory.getLogger(ParquetScheme.class);

    public static final String PARQUET_INSTANT_DEFAULT = "polyphonic.parquet.instant.default";

    static Map<Type, PrimitiveType.PrimitiveTypeName> nativeToPrimitive = new IdentityHashMap<>();
    static Map<PrimitiveType.PrimitiveTypeName, Type> requiredPrimitiveToNative = new IdentityHashMap<>();
    static Map<PrimitiveType.PrimitiveTypeName, Type> optionalPrimitiveToNative = new IdentityHashMap<>();

    static {
        nativeToPrimitive.put(Integer.class, PrimitiveType.PrimitiveTypeName.INT32);
        nativeToPrimitive.put(Integer.TYPE, PrimitiveType.PrimitiveTypeName.INT32);
        nativeToPrimitive.put(Long.class, PrimitiveType.PrimitiveTypeName.INT64);
        nativeToPrimitive.put(Long.TYPE, PrimitiveType.PrimitiveTypeName.INT64);
        nativeToPrimitive.put(Float.class, PrimitiveType.PrimitiveTypeName.FLOAT);
        nativeToPrimitive.put(Float.TYPE, PrimitiveType.PrimitiveTypeName.FLOAT);
        nativeToPrimitive.put(Double.class, PrimitiveType.PrimitiveTypeName.DOUBLE);
        nativeToPrimitive.put(Double.TYPE, PrimitiveType.PrimitiveTypeName.DOUBLE);
        nativeToPrimitive.put(Boolean.class, PrimitiveType.PrimitiveTypeName.BOOLEAN);
        nativeToPrimitive.put(Boolean.TYPE, PrimitiveType.PrimitiveTypeName.BOOLEAN);

        requiredPrimitiveToNative.put(PrimitiveType.PrimitiveTypeName.INT32, Integer.TYPE);
        requiredPrimitiveToNative.put(PrimitiveType.PrimitiveTypeName.INT64, Long.TYPE);
        requiredPrimitiveToNative.put(PrimitiveType.PrimitiveTypeName.FLOAT, Float.TYPE);
        requiredPrimitiveToNative.put(PrimitiveType.PrimitiveTypeName.DOUBLE, Double.TYPE);
        requiredPrimitiveToNative.put(PrimitiveType.PrimitiveTypeName.BOOLEAN, Boolean.TYPE);

        optionalPrimitiveToNative.put(PrimitiveType.PrimitiveTypeName.INT32, Integer.class);
        optionalPrimitiveToNative.put(PrimitiveType.PrimitiveTypeName.INT64, Long.class);
        optionalPrimitiveToNative.put(PrimitiveType.PrimitiveTypeName.FLOAT, Float.class);
        optionalPrimitiveToNative.put(PrimitiveType.PrimitiveTypeName.DOUBLE, Double.class);
        optionalPrimitiveToNative.put(PrimitiveType.PrimitiveTypeName.BOOLEAN, Boolean.class);
    }

    protected FilterPredicate filterPredicate;
    protected CompressionCodecName compressionCodecName = CompressionCodecName.UNCOMPRESSED;

    protected String parquetSchema;
    protected boolean hasRetrievedFields = false;

    public ParquetScheme(Fields fields) {
        super(fields, fields, null);

        this.parquetSchema = createSchema(fields);
    }

    public ParquetScheme(Fields fields, CompressionCodecName compressionCodecName) {
        super(fields, fields, null);

        this.parquetSchema = createSchema(fields);

        if (compressionCodecName != null) {
            this.compressionCodecName = compressionCodecName;
        }
    }

    public ParquetScheme(Fields fields, FilterPredicate filterPredicate) {
        super(fields, fields, null);

        this.parquetSchema = createSchema(fields);
        this.filterPredicate = filterPredicate;
    }

    public ParquetScheme(Fields fields, FilterPredicate filterPredicate, CompressionCodecName compressionCodecName) {
        super(fields, fields, null);

        this.parquetSchema = createSchema(fields);
        this.filterPredicate = filterPredicate;

        if (compressionCodecName != null) {
            this.compressionCodecName = compressionCodecName;
        }
    }

    @Override
    public synchronized Fields retrieveSourceFields(FlowProcess<? extends JobConf> flowProcess, Tap tap) {
        // we retrieve the fields to capture missing type information
        // note that we merge the 'requested fields' with what we found
        if (hasRetrievedFields) {
            return getSourceFields();
        }

        hasRetrievedFields = true;

        if (tap instanceof PartitionTap) {
            String[] partitionIdentifiers = getChildPartitionIdentifiers(flowProcess, (PartitionTap) tap);

            if (partitionIdentifiers == null || partitionIdentifiers.length == 0) {
                throw new TapException("unable to get partition child identifiers, parent resource may not exist: " + tap.getIdentifier());
            }

            tap = new Hfs(tap.getScheme(), partitionIdentifiers[0]);
        } else if (tap instanceof CompositeTap) {
            tap = (Hfs) ((CompositeTap) tap).getChildTaps().next();
        } else {
            // if we are pointing to a directory of partitions, we only need to see the first file to get the header
            // also, internally parquet will create splits etc and assume we are task side thus filtering out relevant metadata
            String[] childIdentifiers = getChildIdentifiers(flowProcess, (Hfs) tap);

            if (childIdentifiers == null || childIdentifiers.length == 0) {
                throw new TapException("unable to get child identifiers, parent resource may not exist: " + tap.getIdentifier());
            }

            tap = new Hfs(tap.getScheme(), childIdentifiers[0]);
        }

        return retrieveSourceFieldsOverride(flowProcess, tap);
    }

    private String[] getChildIdentifiers(FlowProcess<? extends JobConf> flowProcess, Hfs tap) {
        try {
            return tap.getChildIdentifiers(flowProcess, 10, true);
        } catch (IOException exception) {
            throw new TapException("unable to get child identifiers", exception);
        }
    }

    private String[] getChildPartitionIdentifiers(FlowProcess<? extends JobConf> flowProcess, PartitionTap tap) {
        try {
            return tap.getChildPartitionIdentifiers(flowProcess, true);
        } catch (IOException exception) {
            throw new TapException("unable to get child identifiers", exception);
        }
    }

    protected Fields retrieveSourceFieldsOverride(FlowProcess<? extends JobConf> flowProcess, Tap tap) {
        MessageType schema = readSchema(flowProcess, tap);

        setSourceFields(createFields(flowProcess, schema, getSourceFields()));

        return getSourceFields();
    }

    protected MessageType readSchema(FlowProcess<? extends JobConf> flowProcess, Tap tap) {
        try {
            List<Footer> footers = getFooters(flowProcess, (Hfs) tap);

            if (footers.isEmpty()) {
                throw new TapException("could not read Parquet metadata at " + ((Hfs) tap).getPath());
            } else {
                return footers.get(0).getParquetMetadata().getFileMetaData().getSchema();
            }
        } catch (IOException e) {
            throw new TapException(e);
        }
    }

    protected List<Footer> getFooters(FlowProcess<? extends JobConf> flowProcess, Hfs hfs) throws IOException {
        JobConf jobConf = flowProcess.getConfigCopy();
        DeprecatedParquetInputFormat format = new DeprecatedParquetInputFormat();
        format.addInputPath(jobConf, hfs.getPath());
        return format.getFooters(jobConf);
    }

    protected Fields createFields(FlowProcess<? extends JobConf> flowProcess, MessageType fileSchema, Fields requestedFields) {
        boolean useInstant = flowProcess.getBooleanProperty(PARQUET_INSTANT_DEFAULT, false);

        if (requestedFields.isUnknown()) {
            requestedFields = Fields.ALL;
        }

        for (Comparable requestedField : requestedFields) {
            if (!fileSchema.containsField(requestedField.toString())) {
                throw new TapException("parquet source scheme does not contain: " + requestedField);
            }
        }

        Fields newFields = Fields.NONE;
        int schemaSize = fileSchema.getFieldCount();

        for (int i = 0; i < schemaSize; i++) {
            org.apache.parquet.schema.Type type = fileSchema.getType(i);
            String fieldName = type.getName();

            PrimitiveType primitiveType = type.asPrimitiveType();
            Repetition repetition = type.getRepetition();

            Type javaType;

            if (repetition == Repetition.REQUIRED) {
                javaType = requiredPrimitiveToNative.get(primitiveType.getPrimitiveTypeName());
            } else if (repetition == Repetition.OPTIONAL) {
                javaType = optionalPrimitiveToNative.get(primitiveType.getPrimitiveTypeName());
            } else {
                throw new IllegalStateException("unsupported repetition: " + repetition);
            }

            // if the requested fields request a compatible type, honor it
            Type requestedType = requestedFields.hasTypes() && requestedFields.contains(new Fields(fieldName)) ? requestedFields.getType(fieldName) : null;

            LogicalTypeAnnotation logicalTypeAnnotation = type.getLogicalTypeAnnotation();

            // this map should be parameterized and provided by the scheme creator
            if (javaType == null && primitiveType.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.BINARY) {
                if (logicalTypeAnnotation.equals(LogicalTypeAnnotation.stringType())) {
                    javaType = String.class;
                } else if (logicalTypeAnnotation.equals(LogicalTypeAnnotation.jsonType())) {
                    javaType = JSONUtil.TYPE;
                }
            } else if (javaType != null && logicalTypeAnnotation != null) {
                if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
                    LogicalTypeAnnotation.TimeUnit unit = ((LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) logicalTypeAnnotation).getUnit();

                    if (!((LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) logicalTypeAnnotation).isAdjustedToUTC()) {
                        throw new IllegalStateException("only supports adjusted to UTC timestamp logical types");
                    }

                    if (requestedType instanceof InstantType) {
                        javaType = requestedType;
                    } else {
                        javaType = useInstant ? resolveAsInstantType(javaType, unit) : resolveAsDateType(javaType, unit);
                    }
                }
            }

            if (requestedType != null && javaType == null) {
                javaType = requestedType;
            } else if (requestedType instanceof CoercibleType) {
                Class<?> canonicalType = ((CoercibleType<?>) requestedType).getCanonicalType();

                // allow swapping of CoercibleTypes if they have effectively the same canonical type
                if (javaType instanceof CoercibleType && Coercions.asNonPrimitive(((CoercibleType<?>) javaType).getCanonicalType()) == Coercions.asNonPrimitive(canonicalType)) {
                    javaType = requestedType;
                } else if (Coercions.asNonPrimitive(Coercions.asClass(javaType)) == Coercions.asNonPrimitive(canonicalType)) {
                    javaType = requestedType;
                } else {
                    throw new IllegalStateException(format("requested coercible type: %s, with canonical type: %s, does not match parquet declared type: %s", requestedType, canonicalType, type));
                }
            }

            if (javaType == null) {
                throw new IllegalStateException("unsupported parquet type: " + type);
            }

            Fields name = new Fields(fieldName, javaType);

            if (requestedFields.contains(name)) {
                newFields = newFields.append(name);
            }
        }

        return newFields;
    }

    protected Type resolveAsDateType(Type javaType, LogicalTypeAnnotation.TimeUnit unit) {
        switch (unit) {
            case MILLIS:
                return FieldsUtil.DATE_TYPE;
            case MICROS:
                return FieldsUtil.DATE_TYPE_MICROS;
            case NANOS:
                return FieldsUtil.DATE_TYPE_NANOS;
        }
        return javaType;
    }

    protected Type resolveAsInstantType(Type javaType, LogicalTypeAnnotation.TimeUnit unit) {
        switch (unit) {
            case MILLIS:
                return new InstantType(ChronoUnit.MILLIS);
            case MICROS:
                return new InstantType(ChronoUnit.MICROS);
            case NANOS:
                return new InstantType(ChronoUnit.NANOS);
        }
        return javaType;
    }

    @Override
    protected void presentSinkFieldsInternal(Fields fields) {
        if (getSinkFields().isDefined()) {

            // if we haven't declared types, use those presented
            // if we had declared types, the parguet schema will coerce the values
            // so we want to honor those declared vs presented
            if (getSinkFields().hasTypes()) {
                return;
            }

            fields = getSinkFields().applyTypes(fields);
        }

        setSinkFields(fields);

        parquetSchema = createSchema(fields);
    }

    private String createSchema(Fields sinkFields) {
        if (!sinkFields.isDefined()) {
            return null;
        }

        Types.MessageTypeBuilder builder = Types.buildMessage();

        Iterator<Fields> iterator = sinkFields.fieldsIterator();

        Types.GroupBuilder<MessageType> latest = null;

        while (iterator.hasNext()) {
            Fields next = iterator.next();

            String name = next.get(0).toString();
            Type fieldType = next.getType(0);
            Type type = fieldType;

            if (type == null) {
                type = String.class;
            } else if (type instanceof CoercibleType) {
                type = ((CoercibleType) type).getCanonicalType();
            }

            if (type == String.class) {
                latest = builder.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named(name);
            } else if (nativeToPrimitive.containsKey(type)) { // is a primitive or object base value

                // attempt to honor the type semantics. primitives cannot be null, so are required.
                // that said, a CoercibleType coercion could return nulls even though the canonical type is primitive
                Repetition repetition = ((Class) type).isPrimitive() && !(fieldType instanceof CoercibleType) ? Repetition.REQUIRED : Repetition.OPTIONAL;

                LogicalTypeAnnotation logicalTypeAnnotation = null;

                if (fieldType instanceof DateType) {
                    String pattern = ((DateType) fieldType).getDateFormat().toPattern();
                    LogicalTypeAnnotation.TimeUnit unit = LogicalTypeAnnotation.TimeUnit.MILLIS;

                    if (pattern.matches(".*[^S]SSSZ?$")) {
                        unit = LogicalTypeAnnotation.TimeUnit.MILLIS;
                    } else if (pattern.matches(".*[^S]SSSSSSZ?$")) {
                        unit = LogicalTypeAnnotation.TimeUnit.MICROS;
                    } else if (pattern.matches(".*[^S]SSSSSSSSSZ?$")) {
                        unit = LogicalTypeAnnotation.TimeUnit.NANOS;
                    }

                    boolean isAdjustedToUTC = true; // DateType always adjusts the value into UTC, or assumes its UTC if no zone given

                    logicalTypeAnnotation = LogicalTypeAnnotation.timestampType(isAdjustedToUTC, unit);
                }

                latest = builder.primitive(nativeToPrimitive.get(type), repetition).as(logicalTypeAnnotation).named(name);
            } else if (type == Instant.class) {
                LogicalTypeAnnotation.TimeUnit unit = LogicalTypeAnnotation.TimeUnit.MILLIS;

                if (fieldType instanceof InstantType) {
                    TemporalUnit temporalUnit = ((InstantType) fieldType).getUnit();

                    // millis set above as default
                    if (ChronoUnit.MICROS.equals(temporalUnit)) {
                        unit = LogicalTypeAnnotation.TimeUnit.MICROS;
                    } else if (ChronoUnit.NANOS.equals(temporalUnit)) {
                        unit = LogicalTypeAnnotation.TimeUnit.NANOS;
                    } else if (ChronoUnit.SECONDS.equals(temporalUnit)) {
                        throw new IllegalArgumentException("parquet does not support timestamps with SECOND precision");
                    }
                }

                LogicalTypeAnnotation logicalTypeAnnotation = LogicalTypeAnnotation.timestampType(true, unit);
                PrimitiveType.PrimitiveTypeName precision = PrimitiveType.PrimitiveTypeName.INT64;

                latest = builder.primitive(precision, Repetition.OPTIONAL).as(logicalTypeAnnotation).named(name);
            } else if (fieldType instanceof JSONCoercibleType) {
                latest = builder.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.jsonType()).named(name);
            } else {
                throw new IllegalArgumentException("unsupported type: " + type.getTypeName() + " on field: " + name);
            }
        }

        if (latest == null) {
            throw new IllegalStateException("unable to generate schema from: " + sinkFields);
        }

        MessageType messageType = latest.named("tuple");

        return messageType.toString();
    }

    @Override
    public void sourceConfInit(FlowProcess<? extends
            JobConf> fp, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf jobConf) {

        if (filterPredicate != null) {
            ParquetInputFormat.setFilterPredicate(jobConf, filterPredicate);
        }

        jobConf.setInputFormat(DeprecatedParquetInputFormat.class);
        // this implementation of read support will fail if a requested fields is not found in the schema
        // use the base class TupleReadSupport for the original behavior
        ParquetInputFormat.setReadSupportClass(jobConf, CheckedTupleReadSupport.class);
        Fields sourceFields = getSourceFields();

        String fieldsString = Joiner.on(':').join(sourceFields.iterator());
        jobConf.set("parquet.cascading.requested.fields", fieldsString);
    }

    @Override
    public void sourcePrepare(FlowProcess<? extends JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall) throws IOException {
        Object value = sourceCall.getInput().createValue();
        BiConsumer<TupleEntry, Tuple> setter = sourceCall.getIncomingEntry().getFields().isUnknown() ? TupleEntry::setTuple : TupleEntry::setCanonicalTuple;
        sourceCall.setContext(new Object[]{value, setter});
    }

    @Override
    public void sourceRePrepare(FlowProcess<? extends JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall) throws IOException {
        Object value = sourceCall.getInput().createValue();
        sourceCall.getContext()[0] = value;
    }

    public boolean source(FlowProcess<? extends JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall) throws IOException {
        Container<Tuple> value = (Container<Tuple>) sourceCall.getContext()[0];
        BiConsumer<TupleEntry, Tuple> setter = (BiConsumer<TupleEntry, Tuple>) sourceCall.getContext()[1];

        while (true) {
            boolean hasNext = sourceCall.getInput().next(null, value);

            // value may be null and hasNext = false, unsure why this happens but is intentional
            // sourceRePrepare catches this case and will construct a new value container on the next
            // partition iteration
            // previously it was thought ParquetInputFormat.TASK_SIDE_METADATA needed to be set to force
            // footers to read metadata, this only used more memory and slowed processing
            if (!hasNext) {
                return false;
            }

            // unsure when value == null and hasNext is true, but we should force a new value object
            // and continue until eof
            if (value == null) {
                value = (Container<Tuple>) sourceCall.getInput().createValue();
                sourceCall.getContext()[0] = value;
                continue;
            }

            Tuple tuple = value.get();

            setter.accept(sourceCall.getIncomingEntry(), tuple);

            return true;
        }
    }

    @Override
    public void sinkConfInit(FlowProcess<? extends JobConf> fp, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf jobConf) {

        if (jobConf.get("mapreduce.task.partition") == null) {
            jobConf.setInt("mapreduce.task.partition", 0);
        }

        jobConf.set(ParquetOutputFormat.COMPRESSION, compressionCodecName.name());
        DeprecatedParquetOutputFormat.setAsOutputFormat(jobConf);
        jobConf.set(TupleWriteSupport.PARQUET_CASCADING_SCHEMA, parquetSchema);
        ParquetOutputFormat.setWriteSupportClass(jobConf, CheckedTupleWriteSupport.class);
    }

    @Override
    public boolean isSink() {
        return true;
    }
    
}
