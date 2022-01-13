/*
 * Copyright (c) 2016-2021 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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

package cascading.tuple.coerce;

import java.lang.reflect.Type;
import java.util.Map;

/**
 *
 */
public class DoubleCoerce extends NumberCoerce<Double>
  {
  protected DoubleCoerce( Map<Type, Coercions.Coerce> map )
    {
    super( map );
    }

  @Override
  public Class<Double> getCanonicalType()
    {
    return double.class;
    }

  @Override
  protected Double forNull()
    {
    return 0D;
    }

  @Override
  protected Double forBoolean( Boolean f )
    {
    return f ? 1D : 0D;
    }

  @Override
  protected <T> Double parseType( T f )
    {
    return Double.parseDouble( f.toString() );
    }

  @Override
  protected Double asType( Number f )
    {
    return f.doubleValue();
    }
  }
