/*
 * Copyright (c) 2008, Your Corporation. All Rights Reserved.
 */

package cascading.tuple.hadoop;

import cascading.tuple.TuplePair;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *
 */
public class ReverseComparator extends WritableComparator
  {
  public ReverseComparator()
    {
    super( TuplePair.class );
    }

  @Override
  public int compare( WritableComparable lhs, WritableComparable rhs )
    {
    return rhs.compareTo( lhs ); // swap arguments
    }
  }