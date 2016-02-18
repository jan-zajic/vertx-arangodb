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

import java.util.logging.Level;

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
import santo.vertx.arangodb.rest.AqlAPI;

/**
 * Integration tests for the {@link santo.vertx.arangodb.rest.AqlAPI} against an external <a href="http://www.arangodb.com">ArangoDB</a> instance
 * 
 * @author sANTo
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AqlIntegrationTest extends BaseIntegrationTest {
    
    public static String idVertex01;
    public static String idVertex02;
    
    @Test
    public void test01ExecuteCursor(TestContext context) {
    	final Async async = context.async();
        System.out.println("*** test01ExecuteCursor ***");
        String query = "FOR v in " + vertexColName + " LIMIT 2 RETURN v";
        JsonObject documentObject = new JsonObject();
        documentObject.put(AqlAPI.DOC_ATTRIBUTE_QUERY, query);
        documentObject.put("count", true);
        JsonObject requestObject = new JsonObject();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_AQL);
        requestObject.put(AqlAPI.MSG_PROPERTY_ACTION, AqlAPI.MSG_ACTION_EXECUTE);
        requestObject.put(AqlAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                	if(reply.cause() != null)
                		logger.log(Level.SEVERE, "test01ExecuteCursor", reply.cause());
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertTrue(!arangoResult.getBoolean("error"), "Aql request resulted in an error: " + arangoResult.getString("errorMessage"));
                    if (!arangoResult.getBoolean("error")) context.assertNotNull(arangoResult.getInteger("count"), "No count number received");
                }
                catch (Exception e) {
                	logger.log(Level.SEVERE, "test01ExecuteCursor", e);
                    context.fail("test01ExecuteCursor");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test02ValidateQuery(TestContext context) {
        System.out.println("*** test02ValidateQuery ***");
        String query = "FOR v in " + vertexColName + " LIMIT 1 RETURN v";
        JsonObject documentObject = new JsonObject();
        documentObject.put(AqlAPI.DOC_ATTRIBUTE_QUERY, query);
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_AQL);
        requestObject.put(AqlAPI.MSG_PROPERTY_ACTION, AqlAPI.MSG_ACTION_VALIDATE);
        requestObject.put(AqlAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertTrue(!arangoResult.getBoolean("error"),"Aql request resulted in an error: " + arangoResult.getString("errorMessage"));
                    if (!arangoResult.getBoolean("error")) context.assertNotNull(arangoResult.getJsonArray("collections"), "No collections found");
                }
                catch (Exception e) {
                    context.fail("test02ValidateQuery");
                }
                context.asyncAssertSuccess();
            }
        });
    }

    @Test
    public void test03ExecuteNext(TestContext context) {
        System.out.println("*** test03ExecuteNext ***");
        String query = "FOR v in " + vertexColName + " LIMIT 2 RETURN v";
        JsonObject documentObject = new JsonObject();
        documentObject.put(AqlAPI.DOC_ATTRIBUTE_QUERY, query);
        documentObject.put("count", true);
        documentObject.put("batchSize", 1);
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_AQL);
        requestObject.put(AqlAPI.MSG_PROPERTY_ACTION, AqlAPI.MSG_ACTION_EXECUTE);
        requestObject.put(AqlAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertTrue(!arangoResult.getBoolean("error"), "Aql request resulted in an error: " + arangoResult.getString("errorMessage"));
                    if (!arangoResult.getBoolean("error")) 
                    	context.assertNotNull(arangoResult.getInteger("count"),"No count number received");
                    context.assertTrue(arangoResult.getBoolean("hasMore"), "No cursor available");
                    String cursorId = arangoResult.getString("id");
                    
                    // cursor ok, now retrieve next batch
                    JsonObject requestObject = new JsonObject(); final Async async = context.async();
                    requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_AQL);
                    requestObject.put(AqlAPI.MSG_PROPERTY_ACTION, AqlAPI.MSG_ACTION_NEXT);
                    requestObject.put(AqlAPI.MSG_PROPERTY_ID, cursorId);
                    vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
                        @Override
                        public void handle(AsyncResult<Message<JsonObject>> reply) {
                            try {
                                JsonObject response = reply.result().body();
                                System.out.println("response: " + response);
                                JsonObject arangoResult = response.getJsonObject("result");
                                context.assertTrue(!arangoResult.getBoolean("error"),"Aql request resulted in an error: " + arangoResult.getString("errorMessage"));
                                if (!arangoResult.getBoolean("error")) context.assertNotNull(arangoResult.getInteger("count"), "No count number received");
                            }
                            catch (Exception e) {
                                context.fail("test03ExecuteNext");
                            }
                            context.asyncAssertSuccess();
                        }
                    });
                    
                }
                catch (Exception e) {
                    context.fail("test03ExecuteNext");
                }
                //context.asyncAssertSuccess();
            }
        });
    }

    @Test
    public void test04DeleteCursor(TestContext context) {
        System.out.println("*** test04DeleteCursor ***");
        String query = "FOR v in " + vertexColName + " LIMIT 2 RETURN v";
        JsonObject documentObject = new JsonObject();
        documentObject.put(AqlAPI.DOC_ATTRIBUTE_QUERY, query);
        documentObject.put("count", true);
        documentObject.put("batchSize", 1);
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_AQL);
        requestObject.put(AqlAPI.MSG_PROPERTY_ACTION, AqlAPI.MSG_ACTION_EXECUTE);
        requestObject.put(AqlAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertTrue(!arangoResult.getBoolean("error"), "Aql request resulted in an error: " + arangoResult.getString("errorMessage"));
                    if (!arangoResult.getBoolean("error")) context.assertNotNull(arangoResult.getInteger("count"), "No count number received");
                    context.assertTrue(arangoResult.getBoolean("hasMore"), "No cursor available");
                    String cursorId = arangoResult.getString("id");
                    
                    // cursor ok, now retrieve next batch
                    JsonObject requestObject = new JsonObject(); final Async async = context.async();
                    requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_AQL);
                    requestObject.put(AqlAPI.MSG_PROPERTY_ACTION, AqlAPI.MSG_ACTION_DELETE);
                    requestObject.put(AqlAPI.MSG_PROPERTY_ID, cursorId);
                    vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
                        @Override
                        public void handle(AsyncResult<Message<JsonObject>> reply) {
                            try {
                                JsonObject response = reply.result().body();
                                System.out.println("response: " + response);
                                JsonObject arangoResult = response.getJsonObject("result");
                                context.assertTrue(!arangoResult.getBoolean("error"), "Aql request resulted in an error: " + arangoResult.getString("errorMessage"));
                                if (!arangoResult.getBoolean("error")) 
                                	context.assertTrue(arangoResult.getInteger("code") == 202, "Wrong return code received: " + arangoResult.getInteger("code"));
                            }
                            catch (Exception e) {
                                context.fail("test04DeleteCursor");
                            }
                            context.asyncAssertSuccess();
                        }
                    });
                    
                }
                catch (Exception e) {
                    context.fail("test04DeleteCursor");
                }
            }
        });
    }

    @Test
    public void test05ExplainQuery(TestContext context) {
        System.out.println("*** test05ExplainQuery ***");
        String query = "FOR v in " + vertexColName + " LIMIT 1 RETURN v";
        JsonObject documentObject = new JsonObject();
        documentObject.put(AqlAPI.DOC_ATTRIBUTE_QUERY, query);
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_AQL);
        requestObject.put(AqlAPI.MSG_PROPERTY_ACTION, AqlAPI.MSG_ACTION_EXPLAIN);
        requestObject.put(AqlAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertTrue(!arangoResult.getBoolean("error"), "Aql request resulted in an error: " + arangoResult.getString("errorMessage"));
                    if (!arangoResult.getBoolean("error")) context.assertTrue(arangoResult.getInteger("code") == 200, "Wrong return code received: " + arangoResult.getInteger("code"));
                }
                catch (Exception e) {
                    context.fail("test05ExplainQuery");
                }
                context.asyncAssertSuccess();
            }
        });
    }

    @Test
    public void test06CreateFunction(TestContext context) {
        System.out.println("*** test06CreateFunction ***");
        String name = "test::mytestfunction";
        String code = "function (celsius) { return celsius * 1.8 + 32; }";
        JsonObject documentObject = new JsonObject();
        documentObject.put(AqlAPI.DOC_ATTRIBUTE_NAME, name);
        documentObject.put(AqlAPI.DOC_ATTRIBUTE_CODE, code);
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_AQL);
        requestObject.put(AqlAPI.MSG_PROPERTY_ACTION, AqlAPI.MSG_ACTION_CREATE_FUNCTION);
        requestObject.put(AqlAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertTrue(!arangoResult.getBoolean("error"), "Aql request resulted in an error: " + arangoResult.getString("errorMessage"));
                    if (!arangoResult.getBoolean("error")) 
                    	context.assertTrue(arangoResult.getInteger("code") == 201, "Wrong return code received: " + arangoResult.getInteger("code"));
                }
                catch (Exception e) {
                    context.fail("test06CreateFunction");
                }
                context.asyncAssertSuccess();
            }
        });
    }

    @Test
    public void test07GetFunctions(TestContext context) {
        System.out.println("*** test07GetFunctions ***");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_AQL);
        requestObject.put(AqlAPI.MSG_PROPERTY_ACTION, AqlAPI.MSG_ACTION_GET_FUNCTION);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("Aql request resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                }
                catch (Exception e) {
                    context.fail("test07GetFunctions");
                }
                context.asyncAssertSuccess();
            }
        });
    }

    @Test
    public void test08GetFunctionsFromNamespace(TestContext context) {
        System.out.println("*** test08GetFunctionsFromNamespace ***");
        String namespace = "test";
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_AQL);
        requestObject.put(AqlAPI.MSG_PROPERTY_ACTION, AqlAPI.MSG_ACTION_GET_FUNCTION);
        requestObject.put(AqlAPI.MSG_PROPERTY_NAMESPACE, namespace);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("Aql request resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                }
                catch (Exception e) {
                    context.fail("test08GetFunctionsFromNamespace");
                }
                context.asyncAssertSuccess();
            }
        });
    }

    @Test
    public void test09DeleteFunction(TestContext context) {
        System.out.println("*** test09DeleteFunction ***");
        String name = "test::mytestfunction";
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_AQL);
        requestObject.put(AqlAPI.MSG_PROPERTY_ACTION, AqlAPI.MSG_ACTION_DELETE_FUNCTION);
        requestObject.put(AqlAPI.MSG_PROPERTY_NAME, name);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertTrue(!arangoResult.getBoolean("error"), "Aql request resulted in an error: " + arangoResult.getString("errorMessage"));
                    if (!arangoResult.getBoolean("error")) 
                    	context.assertTrue(arangoResult.getInteger("code") == 200, "Wrong return code received: " + arangoResult.getInteger("code"));
                }
                catch (Exception e) {
                    context.fail("test09DeleteFunction");
                }
                context.asyncAssertSuccess();
            }
        });
    }

}
