package cascading.tuple.coerce;

import cascading.CascadingTestCase;
import cascading.tuple.coerce.Coercions.Coerce;

public class ShortObjectCoerceTest extends CascadingTestCase 
{

  private final Coerce<Short> coercion = Coercions.SHORT_OBJECT;

  public void testCoerceShort() 
  {
    assertEquals(new Short((short) 1), coercion.coerce(new Short((short) 1)));
  }

  public void testCoerceNumber() 
  {
    assertEquals(new Short((short) 1), coercion.coerce(new Integer(1)));
  }

  public void testCoerceString() 
  {
    assertEquals(new Short((short) 1), coercion.coerce("1"));
  }

  public void testCoerceNull() 
  {
    assertEquals(null, coercion.coerce(null));
  }

  public void testCoerceEmptyString() 
  {
    assertEquals(null, coercion.coerce(""));
  }

}
