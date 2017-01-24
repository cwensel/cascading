package cascading.tuple.coerce;

import cascading.CascadingTestCase;
import cascading.tuple.coerce.Coercions.Coerce;

public class IntegerObjectCoerceTest extends CascadingTestCase 
{

  private final Coerce<Integer> coercion = Coercions.INTEGER_OBJECT;

  public void testCoerceShort() 
  {
    assertEquals(new Integer(1), coercion.coerce(new Integer(1)));
  }

  public void testCoerceNumber() 
  {
    assertEquals(new Integer(1), coercion.coerce(new Long(1L)));
  }

  public void testCoerceString() 
  {
    assertEquals(new Integer(1), coercion.coerce("1"));
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
