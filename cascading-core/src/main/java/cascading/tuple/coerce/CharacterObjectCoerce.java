/*
 * Copyright (c) 2007-2022 The Cascading Authors. All Rights Reserved.
 *
 * Project and contact information: https://cascading.wensel.net/
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

package cascading.tuple.coerce;

import java.lang.reflect.Type;
import java.util.Map;

/**
 *
 */
public class CharacterObjectCoerce extends Coercions.Coerce<Character>
  {
  public CharacterObjectCoerce( Map<Type, Coercions.Coerce> coercions )
    {
    super( coercions );
    }

  @Override
  public Class<Character> getCanonicalType()
    {
    return Character.class;
    }

  @Override
  public Character coerce( Object value )
    {
    if( value == null )
      return null;

    return value.toString().charAt( 0 );
    }
  }
