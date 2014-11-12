package cascading.tuple;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.util.CloseableIterator;
import cascading.util.SingleValueCloseableIterator;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class TupleEntrySchemeIteratorTest {

  @Test
  public void hasNextCalledMultipleTimesReturnsTrue() throws Exception {
    FlowProcess<?> flowProcess = FlowProcess.NULL;
    Scheme<?, ?, ?, ?, ?> scheme = new MockedScheme();
    CloseableIterator<Object> inputIterator = new MockedSingleValueCloseableIterator(new Object());
    @SuppressWarnings("resource")
    TupleEntrySchemeIterator<?, ?> iterator = new TupleEntrySchemeIterator(flowProcess, scheme, inputIterator);
    assertTrue(iterator.hasNext());
    assertTrue(iterator.hasNext());
  }

  public class MockedScheme extends Scheme {
    private int callCount = 0;

    private static final long serialVersionUID = 1L;

    @Override
    public void sourceConfInit(FlowProcess flowProcess, Tap tap, Object conf) {
    }

    @Override
    public void sinkConfInit(FlowProcess flowProcess, Tap tap, Object conf) {
    }

    @Override
    public boolean source(FlowProcess flowProcess, SourceCall sourceCall) throws IOException {
      // Mimicking a call to read a tuple that throws an exception and any subsequent call will just return false
      // indicating there are no more tuples left to read
      if (callCount == 0) {
        callCount++;
        throw new IOException("Error getting tuple");
      }
      return false;
    }

    @Override
    public void sink(FlowProcess flowProcess, SinkCall sinkCall) throws IOException {

    }

  }

  public class MockedSingleValueCloseableIterator extends SingleValueCloseableIterator<Object> {

    public MockedSingleValueCloseableIterator(Object value) {
      super(value);
    }

    @Override
    public void close() throws IOException {

    }
  }
}
