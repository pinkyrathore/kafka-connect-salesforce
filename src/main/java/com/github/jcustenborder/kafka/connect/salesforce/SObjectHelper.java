/**
 * Copyright © 2016 Jeremy Custenborder (jcustenborder@gmail.com)
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
package com.github.jcustenborder.kafka.connect.salesforce;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.github.jcustenborder.kafka.connect.salesforce.rest.model.SObjectDescriptor;
import com.github.jcustenborder.kafka.connect.utils.data.Parser;
import com.github.jcustenborder.kafka.connect.utils.data.type.DateTypeParser;
import com.google.api.client.util.Preconditions;
import com.google.common.collect.ImmutableMap;

class SObjectHelper {
  static final Parser PARSER;
  static final Map<String, ?> SOURCE_PARTITIONS = new HashMap<>();
  private static final Logger log = LoggerFactory.getLogger(SObjectHelper.class);
  private static final String TOPIC_NAME_SPLIT_REGEX = "(?=[^\\}]*(?:\\{|$))";
  private static final int ONE = 1;
  private static final int TWO = 2;
  private static final String ESCAPE_CHAR = "\\";
  private static final String DOT = ".";
  private static final String HYPHEN = "-";
  private static final String DOLLAR = "$";

  static {
    Parser p = new Parser();
//    "2016-08-15T22:07:59.000Z"
    p.registerTypeParser(Timestamp.SCHEMA, new DateTypeParser(TimeZone.getTimeZone("UTC"), new SimpleDateFormat("YYYY-MM-dd'T'HH:mm:ss.SSS'Z'")));
    PARSER = p;
  }

  public static boolean isTextArea(SObjectDescriptor.Field field) {
    return "textarea".equalsIgnoreCase(field.type());
  }

  public static Schema schema(SObjectDescriptor.Field field) {
    SchemaBuilder builder = null;

    boolean optional = true;

    switch (field.type()) {
      case "id":
        optional = false;
        builder = SchemaBuilder.string().doc("Unique identifier for the object.");
        break;
      case "boolean":
        builder = SchemaBuilder.bool();
        break;
      case "date":
        builder = Date.builder();
        break;
      case "address":
        builder = SchemaBuilder.string();
        break;
      case "string":
        builder = SchemaBuilder.string();
        break;
      case "double":
        builder = SchemaBuilder.float64();
        break;
      case "picklist":
        builder = SchemaBuilder.string();
        break;
      case "textarea":
        builder = SchemaBuilder.string();
        break;
      case "url":
        builder = SchemaBuilder.string();
        break;
      case "int":
        builder = SchemaBuilder.int32();
        break;
      case "reference":
        builder = SchemaBuilder.string();
        break;
      case "datetime":
        builder = Timestamp.builder();
        break;
      case "phone":
        builder = SchemaBuilder.string();
        break;
      case "currency":
        builder = SchemaBuilder.string();
        break;
      case "email":
        builder = SchemaBuilder.string();
        break;
      case "decimal":
        builder = Decimal.builder(field.scale());
        break;
      default:
        throw new UnsupportedOperationException(
            String.format("Field type '%s' for field '%s' is not supported", field.type(), field.name())
        );
    }

    if (optional) {
      builder = builder.optional();
    }

    return builder.build();
  }

  public static Schema valueSchema(SObjectDescriptor descriptor) {
    String name = String.format("%s.%s", SObjectHelper.class.getPackage().getName(), descriptor.name());
    SchemaBuilder builder = SchemaBuilder.struct();
    builder.name(name);

    for (SObjectDescriptor.Field field : descriptor.fields()) {
      if (isTextArea(field)) {
        continue;
      }
      Schema schema = schema(field);
      builder.field(field.name(), schema);
    }

    return builder.build();
  }

  public static Schema keySchema(SObjectDescriptor descriptor) {
    String name = String.format("%s.%sKey", SObjectHelper.class.getPackage().getName(), descriptor.name());
    SchemaBuilder builder = SchemaBuilder.struct();
    builder.name(name);

    SObjectDescriptor.Field keyField = null;

    for (SObjectDescriptor.Field field : descriptor.fields()) {
      if ("id".equalsIgnoreCase(field.type())) {
        keyField = field;
        break;
      }
    }

    if (null == keyField) {
      throw new IllegalStateException("Could not find an id field for " + descriptor.name());
    }

    Schema keySchema = schema(keyField);
    builder.field(keyField.name(), keySchema);
    return builder.build();
  }

  public static void convertStruct(JsonNode data, Schema schema, Struct struct) {
    for (Field field : schema.fields()) {
      String fieldName = field.name();
      JsonNode valueNode = data.findValue(fieldName);
      Object value = PARSER.parseJsonNode(field.schema(), valueNode);
      struct.put(field, value);
    }
  }

  public static SourceRecord convert(SalesforceSourceConfig config, JsonNode jsonNode, Schema keySchema, Schema valueSchema) {
    String topic = config.kafkaTopic();
    Preconditions.checkNotNull(jsonNode);
    Preconditions.checkState(jsonNode.isObject());
    JsonNode dataNode = jsonNode.get("data");
    JsonNode eventNode = dataNode.get("event");
    JsonNode sobjectNode = dataNode.get("sobject");
    long replayId = eventNode.get("replayId").asLong();
    topic = parseTopicName(topic, config.topicNameDelimiter(), dataNode);
    Struct keyStruct = new Struct(keySchema);
    Struct valueStruct = new Struct(valueSchema);
    convertStruct(sobjectNode, keySchema, keyStruct);
    convertStruct(sobjectNode, valueSchema, valueStruct);
    Map<String, Long> sourceOffset = ImmutableMap.of(config.salesForcePushTopicName(), replayId);
    return new SourceRecord(SOURCE_PARTITIONS, sourceOffset, topic, keySchema, keyStruct, valueSchema, valueStruct);
  }

  private static String parseTopicName(String topic, String delimiter, JsonNode dataNode) {
    /*
     * Valid delimiters are : 
     * DOT (,)
     * UNDERSCORE (_)
     * HYPHEN (-)
     * */
    String splitDelimiter = delimiter;
    if (DOT.equals(delimiter) || HYPHEN.equals(delimiter)) {
      splitDelimiter = ESCAPE_CHAR + delimiter; // \\. or \\-
    }

    /*
     * Example 1 - 
     * kafka topic name : test.product.{event.type}
     * Delimiter : . (DOT)
     * Output of split :  [test, product, {event.type}]
     * 
     * Example 2 -
     * kafka topic name : test-product-{event.type}
     * Delimiter : - (HYPHEN)
     * Output of split :  [test, product, {event.type}]
     * 
     */
    String[] kafkaTopicSplit = topic.split(splitDelimiter + TOPIC_NAME_SPLIT_REGEX);
    StringBuilder kafkaTopicNameBuilder = new StringBuilder();
    int idx = 0;
    for (String token : kafkaTopicSplit) {
      if (token.startsWith(DOLLAR)) {
        replaceText(dataNode, kafkaTopicNameBuilder, token);  // e.g. token = ${event.type}
      } else {
        kafkaTopicNameBuilder.append(token);
      }
      if (idx < kafkaTopicSplit.length - ONE) {
        kafkaTopicNameBuilder.append(delimiter);    // join literals by the same delimiter
      }
      idx++;
    }
    return kafkaTopicNameBuilder.toString();
  }

  private static void replaceText(JsonNode dataNode, StringBuilder kafkaTopicNameBuilder, String token) {
    String path = token.substring(TWO, token.length() - ONE); // path = event.type
    JsonNode value = readValue(dataNode, path);
    if (value instanceof TextNode) {
      kafkaTopicNameBuilder.append(value.asText());
    } else {
      kafkaTopicNameBuilder.append(token);
    }
  }

  private static JsonNode readValue(JsonNode source, String path) {
    JsonNode value = null;
    String[] tokens = path.split(ESCAPE_CHAR +  DOT); // path = [event, type]
    JsonNode current = source;
    for (int idx = 0; idx < tokens.length; idx++) {
      String key = tokens[idx];
      value = current.get(key);
      current = value;
    }
    return value;
  }
}
