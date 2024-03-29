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

package cascading.tap.parquet;

import java.util.Map;

import cascading.tap.parquet.convert.TupleRecordMaterializer;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

public class TupleReadSupport extends ReadSupport<Tuple>
  {
  static final String PARQUET_CASCADING_REQUESTED_FIELDS = "parquet.cascading.requested.fields";

  static protected Fields getRequestedFields( Configuration configuration )
    {
    String fieldsString = configuration.get( PARQUET_CASCADING_REQUESTED_FIELDS );

    if( fieldsString == null )
      return Fields.ALL;

    String[] parts = StringUtils.split( fieldsString, ":" );
    if( parts.length == 0 )
      return Fields.ALL;
    else
      return new Fields( parts );
    }

  static protected void setRequestedFields( JobConf configuration, Fields fields )
    {
    String fieldsString = StringUtils.join( fields.iterator(), ":" );
    configuration.set( PARQUET_CASCADING_REQUESTED_FIELDS, fieldsString );
    }

  @Override
  public ReadContext init( Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema )
    {
    Fields requestedFields = getRequestedFields( configuration );
    if( requestedFields == null )
      {
      return new ReadContext( fileSchema );
      }
    else
      {
      SchemaIntersection intersection = new SchemaIntersection( fileSchema, requestedFields );
      return new ReadContext( intersection.getRequestedSchema() );
      }
    }

  @Override
  public RecordMaterializer<Tuple> prepareForRead(
    Configuration configuration,
    Map<String, String> keyValueMetaData,
    MessageType fileSchema,
    ReadContext readContext )
    {
    MessageType requestedSchema = readContext.getRequestedSchema();
    return new TupleRecordMaterializer( requestedSchema );
    }

  }
