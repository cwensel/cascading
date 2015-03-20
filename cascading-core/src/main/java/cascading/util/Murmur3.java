/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.util;

/**
 * Murmur3 is a fast non cryptographic hash algorithm. This class implements the 32bit version of Murmur 3.
 * <p/>
 * The code has been taken from the guava project:
 * https://github.com/google/guava/blob/v18.0/guava/src/com/google/common/hash/Murmur3_32HashFunction.java
 */

/*
* Copyright (C) 2011 The Guava Authors
*
* Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
* in compliance with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software distributed under the License
* is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
* or implied. See the License for the specific language governing permissions and limitations under
* the License.
*/
/*
* MurmurHash3 was written by Austin Appleby, and is placed in the public
* domain. The author hereby disclaims copyright to this source code.
*/
/*
* Source:
* http://code.google.com/p/smhasher/source/browse/trunk/MurmurHash3.cpp
* (Modified to adapt to Guava coding conventions and to use the HashFunction interface)
*/
public class Murmur3
  {
  private static final int C1 = 0xcc9e2d51;
  private static final int C2 = 0x1b873593;
  public static final int SEED = 0x9747b28c;

  /**
   * Applies murmur3 to the given input.
   *
   * @param input The int to hash.
   * @return a murmur3 hash.
   */
  public static int hashInt( int input )
    {
    int k1 = mixK1( input );
    int h1 = mixH1( SEED, k1 );
    return fmix( h1, 4 );
    }

  public static int mixK1( int k1 )
    {
    k1 *= C1;
    k1 = Integer.rotateLeft( k1, 15 );
    k1 *= C2;
    return k1;
    }

  public static int mixH1( int h1, int k1 )
    {
    h1 ^= k1;
    h1 = Integer.rotateLeft( h1, 13 );
    h1 = h1 * 5 + 0xe6546b64;
    return h1;
    }

  // Finalization mix - force all bits of a hash block to avalanche
  public static int fmix( int h1, int length )
    {
    h1 ^= length;
    h1 ^= h1 >>> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >>> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >>> 16;
    return h1;
    }
  }
