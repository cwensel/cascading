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
public class LongCoerce extends NumberCoerce<Long>
  {
  protected LongCoerce( Map<Type, Coercions.Coerce> map )
    {
    super( map );
    }

  @Override
  public Class<Long> getCanonicalType()
    {
    return long.class;
    }

  @Override
  protected Long forNull()
    {
    return 0L;
    }

  @Override
  protected Long forBoolean( Boolean f )
    {
    return f ? 1L : 0L;
    }

  @Override
  protected <T> Long parseType( T f )
    {
    return Long.parseLong( f.toString() );
    }

  @Override
  protected Long asType( Number f )
    {
    return f.longValue();
    }
  }
