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
public class FloatObjectCoerce extends NumberCoerce<Float>
  {
  protected FloatObjectCoerce( Map<Type, Coercions.Coerce> map )
    {
    super( map );
    }

  @Override
  public Class<Float> getCanonicalType()
    {
    return Float.class;
    }

  @Override
  protected Float forNull()
    {
    return null;
    }

  @Override
  protected Float forBoolean( Boolean f )
    {
    return f ? 1F : 0F;
    }

  @Override
  protected <T> Float parseType( T f )
    {
    return Float.parseFloat( f.toString() );
    }

  @Override
  protected Float asType( Number f )
    {
    return f.floatValue();
    }
  }
