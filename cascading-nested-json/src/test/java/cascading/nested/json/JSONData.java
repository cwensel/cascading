/*
 * Copyright (c) 2016-2019 Chris K Wensel. All Rights Reserved.
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

package cascading.nested.json;

/**
 *
 */
public interface JSONData
  {
  String simple = "{ \"existing\":\"value\" }";

  String nested = "{" +
    "\"person\":{" +
    "\"name\":\"John Doe\"," +
    "\"firstName\":\"John\"," +
    "\"lastName\":\"Doe\"," +
    "\"age\":50," +
    "\"human\":true," +
    "\"city\":\"Houston\"," +
    "\"ssn\":\"123-45-6789\"," +
    "\"measure\": { \"value\":100 }," +
    "\"measures\":[ { \"value\":1000 }, { \"value\":2000 } ]," +
    "\"measured\":[ 1000, 2000 ]," +
    "\"children\":[" +
    "\"Jane\"," +
    "\"June\"," +
    "\"Josh\"" +
    "]," +
    "\"arrays\":[" +
    "[" +
    "\"Jane1\"," +
    "\"June1\"," +
    "\"Josh1\"" +
    "]," +
    "[" +
    "\"Jane2\"," +
    "\"June2\"," +
    "\"Josh2\"" +
    "]" +
    "]," +
    "\"zero\": { \"zeroValue\":0 }" +
    "}}";

  String people = "{" +
    "\"people\":" +
    "[ " +
    "{" +
    "\"person\":" +
    "{" +
    "\"name\":\"John Doe\"," +
    "\"firstName\":\"John\"," +
    "\"lastName\":\"Doe\"," +
    "\"age\":50," +
    "\"female\":false," +
    "\"city\":\"Houston\"," +
    "\"ssn\":\"123-45-6789\"" +
    "}}," +
    "{" +
    "\"person\":" +
    "{" +
    "\"name\":\"Jane Doe\"," +
    "\"firstName\":\"Jane\"," +
    "\"lastName\":\"Doe\"," +
    "\"age\":49," +
    "\"female\":true," +
    "\"city\":\"Houston\"," +
    "\"ssn\":\"123-45-6789\"" +
    "}}" +
    "]" +
    "}";

  String nestedArray = "{\n" +
    "\"annotations\": [\n" +
    "{\n" +
    "\"name\": \"begin\",\n" +
    "\"value\": 1570476797161000\n" +
    "},\n" +
    "{\n" +
    "\"name\": \"end\",\n" +
    "\"value\": 1570476797161001\n" +
    "}\n" +
    "]\n" +
    "}\n";

  // values w/ no spaces to simplify test
  String[] objects = new String[]{
    "{ \"name\":\"John\", \"age\":50, \"car\":null }",
    "{\n\"person\":{ \"name\":\"John\", \"age\":50, \"city\":\"Houston\" }\n}",
    "{ \"age\":50 }",
    "{ \"sale\":true }"
  };

  String[] arrays = new String[]{
    "[ \"Ford\", \"BMW\", \"Fiat\" ]",
    "[ [ \"Ford\", \"BMW\", \"Fiat\" ], [ \"Ford\", \"BMW\", \"Fiat\" ], [ \"Ford\", \"BMW\", \"Fiat\" ] ]"
  };
  }