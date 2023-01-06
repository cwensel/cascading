package cascading.operation.bloom;

@FunctionalInterface
public interface IOBiConsumer<T, U> {
    void accept(T t, U u) throws IOException;
    
}
