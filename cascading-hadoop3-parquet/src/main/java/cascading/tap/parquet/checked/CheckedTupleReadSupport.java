package cascading.tap.parquet.checked;

public class CheckedTupleReadSupport  extends TupleReadSupport {
    public CheckedTupleReadSupport() {
    }

    @Override
    public ReadContext init(Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema) {
        Fields requestedFields = getRequestedFields(configuration);
        if (requestedFields == null || requestedFields.isAll()) {
            return new ReadContext(fileSchema);
        } else {
            SchemaIntersection intersection = new SchemaIntersection(fileSchema, requestedFields);

            Fields sourceFields = intersection.getSourceFields();

            // if any of the requested fields are not found, fail
            if (!sourceFields.equalsFields(requestedFields)) {

                Set<String> sourceSet = FieldsUtil.safeNamesSet(sourceFields);
                Set<String> requestedSet = FieldsUtil.safeNamesSet(requestedFields);

                Sets.SetView<String> foundFields = Sets.intersection(sourceSet, requestedSet);
                Sets.SetView<String> missingFields = Sets.difference(requestedSet, sourceSet);

                if(!missingFields.isEmpty()) {
                    throw new IllegalArgumentException("requested parquet fields were not found in the schema, not found: " + missingFields + ", found: " + foundFields);
                }
            }

            return new ReadContext(intersection.getRequestedSchema());
        }
    }
    
}
