package cascading.flow.hadoop.util;

import java.io.IOException;

/** User: sritchie Date: 7/9/12 Time: 10:43 AM */
public interface FlowSerializer
  {
  public <T> byte[] serialize( T object, boolean compress) throws IOException;
  public <T> T deserialize( byte[] bytes, Class<T> klass, boolean decompress) throws IOException;
  public <T> boolean accepts(Class<T> klass);
  }