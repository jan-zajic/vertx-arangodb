/*
 * Copyright 2014 sANTo.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package santo.vertx.arangodb.integration;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import santo.vertx.arangodb.ArangoPersistor;
import santo.vertx.arangodb.rest.GenericAPI;
/**
 * Integration tests for the {@link santo.vertx.arangodb.rest.GenericAPI} against an external <a href="http://www.arangodb.com">ArangoDB</a> instance
 * 
 * @author sANTo
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class GenericIntegrationTest extends BaseIntegrationTest {
    
    public static String idTestDoc;
    public static String revTestDoc;
    
    @Test
    public void test01PerformPostRequest(TestContext context) {
        System.out.println("*** test01PerformPostRequest ***");
        String path = "/document/?collection=" + vertexColName;
        JsonObject documentObject = new JsonObject().put("name", "POST test document");
        documentObject.put("age", 30);
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GENERIC);
        requestObject.put(GenericAPI.MSG_PROPERTY_ACTION, GenericAPI.MSG_ACTION_POST);
        requestObject.put(GenericAPI.MSG_PROPERTY_BODY, documentObject);
        requestObject.put(GenericAPI.MSG_PROPERTY_PATH, path);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("The POST request status was NOT OK: " + response.getString("message"), "ok", response.getString("status"));
                    context.assertFalse(arangoResult.getBoolean("error"), "The POST request resulted in an error: " + arangoResult.getString("errorMessage"));
                    if (!arangoResult.getBoolean("error")) context.assertNotNull(arangoResult.getString("_id"), "No document id received");
                    
                    idTestDoc = arangoResult.getString("_id");
                }
                catch (Exception e) {
                    context.fail("test01PerformPostRequest");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test02PerformGetRequest(TestContext context) {
        System.out.println("*** test02PerformGetRequest ***");
        String path = "/document/" + idTestDoc;
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GENERIC);
        requestObject.put(GenericAPI.MSG_PROPERTY_ACTION, GenericAPI.MSG_ACTION_GET);
        requestObject.put(GenericAPI.MSG_PROPERTY_PATH, path);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("The GET request status was NOT OK: " + response.getString("message"), "ok", response.getString("status"));
                    //context.assertFalse(arangoResult.getBoolean("error"), "The GET request returned an error: " + arangoResult.getString("errorMessage"));
                    //context.assertTrue(arangoResult.getInteger("code") == 200, "Wrong return code received: " + arangoResult.getInteger("code"));
                    System.out.println("result details: " + arangoResult);
                }
                catch (Exception e) {
                    context.fail("test02PerformGetRequest");
                }
                async.complete();
            }
        });
    }
    
    @Test
    public void test03PerformHeadRequest(TestContext context) {
        System.out.println("*** test03PerformHeadRequest ***");
        String path = "/document/" + idTestDoc;
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GENERIC);
        requestObject.put(GenericAPI.MSG_PROPERTY_ACTION, GenericAPI.MSG_ACTION_HEAD);
        requestObject.put(GenericAPI.MSG_PROPERTY_PATH, path);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("The HEAD request status was NOT OK: " + response.getString("message"), "ok", response.getString("status"));
                    //context.assertFalse(arangoResult.getBoolean("error"), "The HEAD request returned an error: " + arangoResult.getString("errorMessage"));
                    //context.assertTrue(arangoResult.getInteger("code") == 200, "Wrong return code received: " + arangoResult.getInteger("code"));
                    System.out.println("result details: " + arangoResult);
                }
                catch (Exception e) {
                    context.fail("test03PerformHeadRequest");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test04PerformPatchRequest(TestContext context) {
        System.out.println("*** test04PerformPatchRequest ***");
        String path = "/document/" + idTestDoc;
        JsonObject documentObject = new JsonObject().put("name", "PATCH test document");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GENERIC);
        requestObject.put(GenericAPI.MSG_PROPERTY_ACTION, GenericAPI.MSG_ACTION_PATCH);
        requestObject.put(GenericAPI.MSG_PROPERTY_BODY, documentObject);
        requestObject.put(GenericAPI.MSG_PROPERTY_PATH, path);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("The PATCH request status was NOT OK: " + response.getString("message"), "ok", response.getString("status"));
                    context.assertFalse(arangoResult.getBoolean("error"), "The PATCH request resulted in an error: " + arangoResult.getString("errorMessage"));
                    if (!arangoResult.getBoolean("error")) context.assertNotNull(arangoResult.getString("_id"), "No document id received");
                    
                    revTestDoc = arangoResult.getString("_rev");
                }
                catch (Exception e) {
                    context.fail("test04PerformPatchRequest");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test05PerformPutRequest(TestContext context) {
        System.out.println("*** test05PerformPutRequest ***");
        String path = "/document/" + idTestDoc;
        JsonObject documentObject = new JsonObject().put("name", "PUT test document");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GENERIC);
        requestObject.put(GenericAPI.MSG_PROPERTY_ACTION, GenericAPI.MSG_ACTION_PUT);
        requestObject.put(GenericAPI.MSG_PROPERTY_BODY, documentObject);
        requestObject.put(GenericAPI.MSG_PROPERTY_PATH, path);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("The PUT request status was NOT OK: " + response.getString("message"), "ok", response.getString("status"));
                    context.assertFalse(arangoResult.getBoolean("error"), "The PUT request resulted in an error: " + arangoResult.getString("errorMessage"));
                    if (!arangoResult.getBoolean("error")) context.assertNotNull(arangoResult.getString("_id"), "No document id received");
                    context.assertNotEquals(revTestDoc,arangoResult.getString("_rev"),"Document not correctly replaced");
                }
                catch (Exception e) {
                    context.fail("test05PerformPutRequest");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test06PerformDeleteRequest(TestContext context) {
        System.out.println("*** test06PerformDeleteRequest ***");
        String path = "/document/" + idTestDoc;
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GENERIC);
        requestObject.put(GenericAPI.MSG_PROPERTY_ACTION, GenericAPI.MSG_ACTION_DELETE);
        requestObject.put(GenericAPI.MSG_PROPERTY_PATH, path);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("The DELETE request status was NOT OK: " + response.getString("message"), "ok", response.getString("status"));
                    //context.assertFalse(arangoResult.getBoolean("error"), "The DELETE request returned an error: " + arangoResult.getString("errorMessage"));
                    //context.assertTrue(arangoResult.getInteger("code") == 202, "Wrong return code received: " + arangoResult.getInteger("code"));
                    System.out.println("result details: " + arangoResult);
                }
                catch (Exception e) {
                    context.fail("test06PerformDeleteRequest");
                }
                async.complete();
            }
        });
    }
}
