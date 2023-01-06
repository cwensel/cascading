package cascading.operation.bloom;

public class ExposedByteArrayOutputStream  extends ByteArrayOutputStream{
    public ExposedByteArrayOutputStream() {
    }

    public ExposedByteArrayOutputStream(int size) {
        super(size);
    }

    public ByteBuffer toByteBuffer() {
        return ByteBuffer.wrap(buf, 0, count);
    }
    
}
