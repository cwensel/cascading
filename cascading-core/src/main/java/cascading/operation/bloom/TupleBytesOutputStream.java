package cascading.operation.bloom;

public class TupleBytesOutputStream  extends DataOutputStream{
    private static final Map<Class, IOBiConsumer<Object, TupleBytesOutputStream>> coercions = new IdentityHashMap<>();

    static {
        coercions.put(Boolean.class, (o, s) -> s.writeBoolean((Boolean) o));
        coercions.put(Boolean.TYPE, (o, s) -> s.writeBoolean((Boolean) o));
        coercions.put(Byte.class, (o, s) -> s.writeByte((Byte) o));
        coercions.put(Byte.TYPE, (o, s) -> s.writeByte((Byte) o));
        coercions.put(Short.class, (o, s) -> s.writeShort((Short) o));
        coercions.put(Short.TYPE, (o, s) -> s.writeShort((Short) o));
        coercions.put(Integer.class, (o, s) -> s.writeInt((Integer) o));
        coercions.put(Integer.TYPE, (o, s) -> s.writeInt((Integer) o));
        coercions.put(Long.class, (o, s) -> s.writeLong((Long) o));
        coercions.put(Long.TYPE, (o, s) -> s.writeLong((Long) o));
        coercions.put(Float.class, (o, s) -> s.writeFloat((Float) o));
        coercions.put(Float.TYPE, (o, s) -> s.writeFloat((Float) o));
        coercions.put(Double.class, (o, s) -> s.writeDouble((Double) o));
        coercions.put(Double.TYPE, (o, s) -> s.writeDouble((Double) o));
        coercions.put(Character.TYPE, (o, s) -> s.writeChar((Character) o));
        coercions.put(Character.class, (o, s) -> s.writeChar((Character) o));
        coercions.put(String.class, (o, s) -> s.writeChars((String) o));
        coercions.put(Tuple.class, (o, s) -> s.writeAnonTuple((Tuple) o));
        coercions.put(Instant.class, (o, s) -> {
            s.writeLong(((Instant) o).getEpochSecond());
            s.writeLong(((Instant) o).getNano());
        });
    }

    private final IOConsumer<Tuple> writer;
    private IOBiConsumer<Object, TupleBytesOutputStream>[] consumers;

    public TupleBytesOutputStream(Class[] types) {
        this(new ExposedByteArrayOutputStream(), types);
    }

    public TupleBytesOutputStream(ExposedByteArrayOutputStream outputStream, Class[] types) {
        super(outputStream);

        if (types == null) {
            writer = this::writeAnonTuple;
        } else {
            buildConsumers(types);
            writer = this::writeTupleTyped;
        }
    }

    public void buildConsumers(Class[] types) {
        consumers = new IOBiConsumer[types.length];

        for (int i = 0; i < types.length; i++) {
            IOBiConsumer<Object, TupleBytesOutputStream> coercion = coercions.get(types[i]);

            if (coercion == null) {
                throw new IllegalArgumentException("unsupported type: " + types[i]);
            }

            consumers[i] = coercion;
        }
    }

    public void writeTuple(Tuple tuple) throws IOException {
        writer.accept(tuple);
    }

    protected void writeTupleTyped(Tuple tuple) throws IOException {
        for (int i = 0; i < consumers.length; i++) {
            Object value = tuple.getObject(i);

            if (value == null) {
                write(0);
                continue;
            }

            consumers[i].accept(value, this);
        }
    }

    protected void writeAnonTuple(Tuple tuple) throws IOException {
        for (int i = 0; i < tuple.size(); i++) {
            Object value = tuple.getObject(i);

            if (value == null) {
                write(0);
                continue;
            }

            writeTypedObject(value.getClass(), value);
        }
    }

    private void writeTypedObject(Class type, Object value) throws IOException {
        IOBiConsumer<Object, TupleBytesOutputStream> consumer = coercions.get(type);

        if (consumer == null)
            throw new IOException("unknown type: " + Coercions.getTypeNames(new Type[]{type})[0] + ", cannot pack into byte array");

        consumer.accept(value, this);
    }

    public ExposedByteArrayOutputStream getWrapped() {
        return (ExposedByteArrayOutputStream) out;
    }

    public void reset() {
        Flushables.flushQuietly(this);
        getWrapped().reset();
    }

    public ByteBuffer toByteBuffer() {
        Flushables.flushQuietly(this);
        return getWrapped().toByteBuffer();
    }

    public byte[] toByteArray() {
        Flushables.flushQuietly(this);
        return getWrapped().toByteArray();
    }
    
}
