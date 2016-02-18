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
import santo.vertx.arangodb.rest.DocumentAPI;

/**
 * Integration tests for the {@link santo.vertx.arangodb.rest.DocumentAPI} against an external <a href="http://www.arangodb.com">ArangoDB</a> instance
 * 
 * @author sANTo
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DocumentIntegrationTest extends BaseIntegrationTest {
    
    public static String docId = null;
    public static String docRevision = null;

    @Test
    public void test01aCreateTestDocument(TestContext context) {
        System.out.println("*** test01aCreateTestDocument ***");
        JsonObject documentObject = new JsonObject().put("description", "test");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_DOCUMENT);
        requestObject.put(DocumentAPI.MSG_PROPERTY_ACTION, DocumentAPI.MSG_ACTION_CREATE);
        requestObject.put(DocumentAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        requestObject.put(DocumentAPI.MSG_PROPERTY_COLLECTION, vertexColName);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertTrue(!arangoResult.getBoolean("error"), "Document creation resulted in an error: " + arangoResult.getString("errorMessage"));
                    if (!arangoResult.getBoolean("error")) context.assertNotNull(arangoResult.getString("_id"), "No document key received");
                    
                    // Create another document that can be used in later tests
                    JsonObject documentObject = new JsonObject().put("description", "test2");
                    JsonObject requestObject = new JsonObject(); final Async async = context.async();
                    requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_DOCUMENT);
                    requestObject.put(DocumentAPI.MSG_PROPERTY_ACTION, DocumentAPI.MSG_ACTION_CREATE);
                    requestObject.put(DocumentAPI.MSG_PROPERTY_DOCUMENT, documentObject);
                    requestObject.put(DocumentAPI.MSG_PROPERTY_COLLECTION, vertexColName);
                    vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
                        @Override
                        public void handle(AsyncResult<Message<JsonObject>> reply) {
                            try {
                                JsonObject response = reply.result().body();
                                System.out.println("response: " + response);
                                JsonObject arangoResult = response.getJsonObject("result");
                                context.assertTrue(!arangoResult.getBoolean("error"), "Document creation resulted in an error: " + arangoResult.getString("errorMessage"));
                                if (!arangoResult.getBoolean("error")) context.assertNotNull(arangoResult.getString("_id"), "No document key received");

                                docId = arangoResult.getString("_id");
                                docRevision = arangoResult.getString("_rev");
                            }
                            catch (Exception e) {
                                context.fail("test01aCreateTestDocument");
                            }
                            async.complete();
                        }
                    });
                }
                catch (Exception e) {
                    context.fail("test01aCreateTestDocument");
                }
                //async.complete();
            }
        });
    }

    @Test
    public void test01bCreateGeoDocument(TestContext context) {
        System.out.println("*** test01bCreateGeoDocument ***");
        JsonObject documentObject = new JsonObject().put("description", "GEO location test document");
        documentObject.put(DocumentAPI.DOC_ATTRIBUTE_LATITUDE, 1);
        documentObject.put(DocumentAPI.DOC_ATTRIBUTE_LONGITUDE, 1);
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_DOCUMENT);
        requestObject.put(DocumentAPI.MSG_PROPERTY_ACTION, DocumentAPI.MSG_ACTION_CREATE);
        requestObject.put(DocumentAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        requestObject.put(DocumentAPI.MSG_PROPERTY_COLLECTION, vertexColName);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertTrue(!arangoResult.getBoolean("error"), "Document creation resulted in an error: " + arangoResult.getString("errorMessage"));
                    if (!arangoResult.getBoolean("error")) context.assertNotNull(arangoResult.getString("_id"), "No document key received");
                }
                catch (Exception e) {
                    context.fail("test01bCreateGeoDocument");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test01cCreateRangeDocument(TestContext context) {
        System.out.println("*** test01cCreateRangeDocument ***");
        JsonObject documentObject = new JsonObject().put("description", "Range test document (skiplist)");
        documentObject.put("age", 30);
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_DOCUMENT);
        requestObject.put(DocumentAPI.MSG_PROPERTY_ACTION, DocumentAPI.MSG_ACTION_CREATE);
        requestObject.put(DocumentAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        requestObject.put(DocumentAPI.MSG_PROPERTY_COLLECTION, vertexColName);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertTrue(!arangoResult.getBoolean("error"), "Document creation resulted in an error: " + arangoResult.getString("errorMessage"));
                    if (!arangoResult.getBoolean("error")) context.assertNotNull(arangoResult.getString("_id"), "No document key received");
                }
                catch (Exception e) {
                    context.fail("test01cCreateRangeDocument");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test02GetDocument(TestContext context) {
        System.out.println("*** test02GetDocument ***");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_DOCUMENT);
        requestObject.put(DocumentAPI.MSG_PROPERTY_ACTION, DocumentAPI.MSG_ACTION_READ);
        requestObject.put(DocumentAPI.MSG_PROPERTY_ID, docId);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("The retrieval of the specified document resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    //context.assertTrue(arangoResult.getInteger("code") == 200, "The request for the specified document was invalid: (returncode: " + arangoResult.getInteger("code") + ")");
                    System.out.println("document details: " + arangoResult);
                }
                catch (Exception e) {
                    context.fail("test02GetDocument");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test03GetDocumentHeader(TestContext context) {
        System.out.println("*** test03GetDocumentHeader ***");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_DOCUMENT);
        requestObject.put(DocumentAPI.MSG_PROPERTY_ACTION, DocumentAPI.MSG_ACTION_HEAD);
        requestObject.put(DocumentAPI.MSG_PROPERTY_ID, docId);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("The retrieval of the specified document header resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    System.out.println("document details: " + arangoResult);
                }
                catch (Exception e) {
                    context.fail("test03GetDocumentHeader");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test04UpdateDocument(TestContext context) {
        System.out.println("*** test04UpdateDocument ***");
        JsonObject documentObject = new JsonObject().put("description", "updated test");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_DOCUMENT);
        requestObject.put(DocumentAPI.MSG_PROPERTY_ACTION, DocumentAPI.MSG_ACTION_UPDATE);
        requestObject.put(DocumentAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        requestObject.put(DocumentAPI.MSG_PROPERTY_ID, docId);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertTrue(!arangoResult.getBoolean("error"), "Document update resulted in an error: " + arangoResult.getString("errorMessage"));
                    if (!arangoResult.getBoolean("error")) context.assertNotNull(arangoResult.getString("_id"), "No document key received");
                    context.assertNotEquals(docRevision, arangoResult.getString("_rev"),"Document not correctly updated");
                    System.out.println("document details: " + arangoResult);
                    
                    docRevision = arangoResult.getString("_rev");
                }
                catch (Exception e) {
                    context.fail("test04UpdateDocument");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test05ReplaceDocument(TestContext context) {
        System.out.println("*** test05ReplaceDocument ***");
        JsonObject documentObject = new JsonObject().put("description", "replaced test");
        documentObject.put("name", "replacement document");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_DOCUMENT);
        requestObject.put(DocumentAPI.MSG_PROPERTY_ACTION, DocumentAPI.MSG_ACTION_REPLACE);
        requestObject.put(DocumentAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        requestObject.put(DocumentAPI.MSG_PROPERTY_ID, docId);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertTrue(!arangoResult.getBoolean("error"), "Document replacement resulted in an error: " + arangoResult.getString("errorMessage"));
                    if (!arangoResult.getBoolean("error")) context.assertNotNull(arangoResult.getString("_id"), "No document key received");
                    context.assertNotEquals(docRevision, arangoResult.getString("_rev"),"Document not correctly replaced");
                    System.out.println("document details: " + arangoResult);
                }
                catch (Exception e) {
                    context.fail("test05ReplaceDocument");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test06GetDocumentList(TestContext context) {
        System.out.println("*** test06GetDocumentList ***");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_DOCUMENT);
        requestObject.put(DocumentAPI.MSG_PROPERTY_ACTION, DocumentAPI.MSG_ACTION_LIST);
        requestObject.put(DocumentAPI.MSG_PROPERTY_COLLECTION, vertexColName);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("The listing of all the document for the specified collection resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    System.out.println("documents for collection: " + arangoResult);
                }
                catch (Exception e) {
                    context.fail("test06GetDocumentList");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test07DeleteDocument(TestContext context) {
        System.out.println("*** test07DeleteDocument ***");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_DOCUMENT);
        requestObject.put(DocumentAPI.MSG_PROPERTY_ACTION, DocumentAPI.MSG_ACTION_DELETE);
        requestObject.put(DocumentAPI.MSG_PROPERTY_ID, docId);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("The deletion of the specified document resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    //context.assertTrue(arangoResult.getInteger("code") == 200, "The request for the specified document was invalid: (returncode: " + arangoResult.getInteger("code") + ")");
                    System.out.println("response details: " + arangoResult);
                }
                catch (Exception e) {
                    context.fail("test07DeleteDocument");
                }
                async.complete();
            }
        });
    }
}
