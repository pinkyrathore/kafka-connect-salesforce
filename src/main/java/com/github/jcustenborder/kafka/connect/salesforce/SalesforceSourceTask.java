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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jcustenborder.kafka.connect.salesforce.rest.model.ApiVersion;
import com.github.jcustenborder.kafka.connect.salesforce.rest.model.SObjectDescriptor;
import com.github.jcustenborder.kafka.connect.salesforce.rest.model.SObjectMetadata;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.github.jcustenborder.kafka.connect.salesforce.rest.SalesforceRestClient;
import com.github.jcustenborder.kafka.connect.salesforce.rest.SalesforceRestClientFactory;
import com.github.jcustenborder.kafka.connect.salesforce.rest.model.AuthenticationResponse;
import com.github.jcustenborder.kafka.connect.salesforce.rest.model.SObjectsResponse;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.cometd.bayeux.Channel;
import org.cometd.bayeux.Message;
import org.cometd.bayeux.client.ClientSessionChannel;
import org.cometd.client.BayeuxClient;
import org.cometd.client.transport.LongPollingTransport;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;


public class SalesforceSourceTask extends SourceTask implements ClientSessionChannel.MessageListener {
  static final Logger log = LoggerFactory.getLogger(SalesforceSourceTask.class);
  final ConcurrentLinkedDeque<SourceRecord> messageQueue = new ConcurrentLinkedDeque<>();
  SalesforceSourceConfig config;
  SalesforceRestClient salesforceRestClient;
  AuthenticationResponse authenticationResponse;
  SObjectDescriptor descriptor;
  SObjectMetadata metadata;
  ApiVersion apiVersion;
  GenericUrl streamingUrl;
  BayeuxClient streamingClient;
  Schema keySchema;
  Schema valueSchema;
  ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  BayeuxClient createClient() {
    SslContextFactory sslContextFactory = new SslContextFactory();
    HttpClient httpClient = new HttpClient(sslContextFactory);
    httpClient.setConnectTimeout(this.config.connectTimeout());
    try {
      httpClient.start();
    } catch (Exception e) {
      throw new ConnectException("Exception thrown while starting httpClient.", e);
    }

    Map<String, Object> options = new HashMap<>();

    LongPollingTransport transport = new LongPollingTransport(options, httpClient) {

      @Override
      protected void customize(Request request) {
        super.customize(request);
        String headerValue = String.format("Authorization: %s %s", authenticationResponse.tokenType(), authenticationResponse.accessToken());
        request.header("Authorization", headerValue);
      }
    };

    return new BayeuxClient(this.streamingUrl.toString(), transport);
  }

  @Override
  public void start(Map<String, String> map) {
    this.config = new SalesforceSourceConfig(map);
    this.salesforceRestClient = SalesforceRestClientFactory.create(this.config);
    this.authenticationResponse = this.salesforceRestClient.authenticate();

    List<ApiVersion> apiVersions = salesforceRestClient.apiVersions();

    for (ApiVersion v : apiVersions) {
      if (this.config.version().equals(v.version())) {
        apiVersion = v;
        break;
      }
    }

    Preconditions.checkNotNull(apiVersion, "Could not find ApiVersion '%s'", this.config.version());
    salesforceRestClient.apiVersion(apiVersion);

    SObjectsResponse sObjectsResponse = salesforceRestClient.objects();

    if (log.isInfoEnabled()) {
      log.info("Looking for metadata for {}", this.config.salesForceObject());
    }

    for (SObjectMetadata metadata : sObjectsResponse.sobjects()) {
      if (this.config.salesForceObject().equals(metadata.name())) {
        this.descriptor = salesforceRestClient.describe(metadata);
        this.metadata = metadata;
        break;
      }
    }

    //2013-05-06T00:00:00+00:00
    Preconditions.checkNotNull(this.descriptor, "Could not find descriptor for '%s'", this.config.salesForceObject());

    this.keySchema = SObjectHelper.keySchema(this.descriptor);
    this.valueSchema = SObjectHelper.valueSchema(this.descriptor);

    this.streamingUrl = new GenericUrl(this.authenticationResponse.instance_url());
    this.streamingUrl.setRawPath(
        String.format("/cometd/%s", this.apiVersion.version())
    );

    if (log.isInfoEnabled()) {
      log.info("Configuring streaming url to {}", this.streamingUrl);
    }
    this.streamingClient = createClient();
    this.streamingClient.getChannel(Channel.META_HANDSHAKE).addListener(new ClientSessionChannel.MessageListener() {
      @Override
      public void onMessage(ClientSessionChannel clientSessionChannel, Message message) {
        if (log.isInfoEnabled()) {
          log.info("onMessage - {}", message);
        }
        if (!message.isSuccessful()) {
          if (log.isErrorEnabled()) {
            log.error("Error during handshake: {} {}", message.get("error"), message.get("exception"));
          }
        }
      }
    });

    if (log.isInfoEnabled()) {
      log.info("Starting handshake");
    }
    this.streamingClient.handshake();
    if (!this.streamingClient.waitFor(30000, BayeuxClient.State.CONNECTED)) {
      throw new ConnectException("Not connected after 30,000 ms.");
    }

    String channel = String.format("/topic/%s", this.config.salesForcePushTopicName());
    if (log.isInfoEnabled()) {
      log.info("Subscribing to {}", channel);
    }
    this.streamingClient.getChannel(channel).subscribe(this);
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    List<SourceRecord> records = new ArrayList<>(256);

    while (records.isEmpty()) {
      int size = messageQueue.size();

      for (int i = 0; i < size; i++) {
        SourceRecord record = this.messageQueue.poll();

        if (null == record) {
          break;
        }

        records.add(record);
      }

      if (records.isEmpty()) {
        Thread.sleep(100);
      }
    }

    return records;
  }

  @Override
  public void stop() {
    this.streamingClient.disconnect();
  }

  @Override
  public void onMessage(ClientSessionChannel clientSessionChannel, Message message) {
    try {
      String jsonMessage = message.getJSON();
      if (log.isDebugEnabled()) {
        log.debug("message={}", jsonMessage);
      }
      JsonNode jsonNode = objectMapper.readTree(jsonMessage);
      SourceRecord record = SObjectHelper.convert(this.config, jsonNode, keySchema, valueSchema);
      this.messageQueue.add(record);
    } catch (Exception ex) {
      if (log.isErrorEnabled()) {
        log.error("Exception thrown while processing message.", ex);
      }
    }
  }
}