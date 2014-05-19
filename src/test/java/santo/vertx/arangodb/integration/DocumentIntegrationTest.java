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

import java.net.URL;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.testtools.TestVerticle;
import org.vertx.testtools.VertxAssert;
import santo.vertx.arangodb.ArangoPersistor;
import santo.vertx.arangodb.Helper;
import santo.vertx.arangodb.rest.DocumentAPI;

/**
 * Integration tests for the {@link santo.vertx.arangodb.rest.DocumentAPI} against an external <a href="http://www.arangodb.org">ArangoDB</a> instance
 * 
 * @author sANTo
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DocumentIntegrationTest extends TestVerticle {
    
    private static final String DEFAULT_ADDRESS = "santo.vertx.arangodb";
    private static final String DEFAULT_TEST_DB = "testdb";
    
    private Logger logger;
    private final String logPrefix = "";
    private JsonObject config;
    private String address;
    private String dbName;
    
    @Override
    public void start() {
        initialize();
        logger = container.logger();
        config = loadConfig();
        address = Helper.getHelper().getOptionalString(config, "address", DEFAULT_ADDRESS);
        dbName = Helper.getHelper().getOptionalString(config, "dbname", DEFAULT_TEST_DB);
        
        // Deploy our persistor before starting the tests
        deployVerticle(ArangoPersistor.class.getName(), config, 1);
    }
    
    private void deployVerticle(final String vertName, JsonObject vertConfig, int vertInstances) {
        logger.trace(logPrefix + "(deployVerticle) vertName: " + vertName);
        if (vertName == null || vertConfig == null) {
            logger.error(logPrefix + "Unable to deploy the requested verticle because one of the parameters is invalid: " + "Name=" + vertName + ",Config=" + vertConfig);
            return;
        }
        container.deployVerticle(vertName, vertConfig, vertInstances, new AsyncResultHandler<String>() {
            @Override
            public void handle(AsyncResult<String> asyncResult) {
                logger.info(logPrefix + "verticle " + vertName + (asyncResult.succeeded() ? " was deployed successfully !" : " failed to deploy"));
                VertxAssert.assertTrue(asyncResult.succeeded());
                VertxAssert.assertNotNull("Persistor deployment failed", asyncResult.result());
                startTests();
            }
        });
    }
    
    @Test
    public void test01CreateDocument() {
        System.out.println("*** test01CreateDocument ***");
        JsonObject documentObject = new JsonObject().putString("description", "test");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_DOCUMENT);
        requestObject.putString(DocumentAPI.MSG_PROPERTY_ACTION, DocumentAPI.MSG_ACTION_CREATE);
        requestObject.putObject(DocumentAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        requestObject.putString(DocumentAPI.MSG_PROPERTY_COLLECTION, "testcol");
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertTrue("Document creation resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    if (!arangoResult.getBoolean("error")) VertxAssert.assertNotNull("No document key received", arangoResult.getString("_id"));
                    
                    // read the new document
                    test02GetDocument(arangoResult.getString("_id"), arangoResult.getString("_rev"));
                }
                catch (Exception e) {
                    e.printStackTrace();
                    VertxAssert.fail("test01CreateDocument");
                }
                //VertxAssert.testComplete();
            }
        });
    }

    public void test02GetDocument(final String id, final String rev) {
        System.out.println("*** test02GetDocument ***");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_DOCUMENT);
        requestObject.putString(DocumentAPI.MSG_PROPERTY_ACTION, DocumentAPI.MSG_ACTION_READ);
        requestObject.putString(DocumentAPI.MSG_PROPERTY_ID, id);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("The retrieval of the specified document resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    //VertxAssert.assertTrue("The request for the specified document was invalid: (returncode: " + arangoResult.getInteger("code") + ")", arangoResult.getInteger("code") == 200);
                    System.out.println("document details: " + arangoResult);
                    
                    // get the document header
                    test03GetDocumentHeader(id, rev);
                }
                catch (Exception e) {
                    e.printStackTrace();
                    VertxAssert.fail("test02GetDocument");
                }
                //VertxAssert.testComplete();
            }
        });
    }

    public void test03GetDocumentHeader(final String id, final String rev) {
        System.out.println("*** test03GetDocumentHeader ***");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_DOCUMENT);
        requestObject.putString(DocumentAPI.MSG_PROPERTY_ACTION, DocumentAPI.MSG_ACTION_HEAD);
        requestObject.putString(DocumentAPI.MSG_PROPERTY_ID, id);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("The retrieval of the specified document header resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    System.out.println("document details: " + arangoResult);
                    
                    // update the document
                    test04UpdateDocument(id, rev);
                }
                catch (Exception e) {
                    e.printStackTrace();
                    VertxAssert.fail("test03GetDocumentHeader");
                }
                //VertxAssert.testComplete();
            }
        });
    }

    public void test04UpdateDocument(final String id, final String rev) {
        System.out.println("*** test04UpdateDocument ***");
        JsonObject documentObject = new JsonObject().putString("description", "updated test");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_DOCUMENT);
        requestObject.putString(DocumentAPI.MSG_PROPERTY_ACTION, DocumentAPI.MSG_ACTION_UPDATE);
        requestObject.putObject(DocumentAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        requestObject.putString(DocumentAPI.MSG_PROPERTY_ID, id);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertTrue("Document update resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    if (!arangoResult.getBoolean("error")) VertxAssert.assertNotNull("No document key received", arangoResult.getString("_id"));
                    VertxAssert.assertNotSame("Document not correctly updated", rev, arangoResult.getString("_rev"));
                    System.out.println("document details: " + arangoResult);
                    
                    // replace the document
                    test05ReplaceDocument(id, arangoResult.getString("_rev"));
                }
                catch (Exception e) {
                    e.printStackTrace();
                    VertxAssert.fail("test04UpdateDocument");
                }
                //VertxAssert.testComplete();
            }
        });
    }

    public void test05ReplaceDocument(final String id, final String rev) {
        System.out.println("*** test05ReplaceDocument ***");
        JsonObject documentObject = new JsonObject().putString("description", "replaced test");
        documentObject.putString("name", "replacement document");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_DOCUMENT);
        requestObject.putString(DocumentAPI.MSG_PROPERTY_ACTION, DocumentAPI.MSG_ACTION_REPLACE);
        requestObject.putObject(DocumentAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        requestObject.putString(DocumentAPI.MSG_PROPERTY_ID, id);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertTrue("Document replacement resulted in an error: " + arangoResult.getString("errorMessage"), !arangoResult.getBoolean("error"));
                    if (!arangoResult.getBoolean("error")) VertxAssert.assertNotNull("No document key received", arangoResult.getString("_id"));
                    VertxAssert.assertNotSame("Document not correctly replaced", rev, arangoResult.getString("_rev"));
                    System.out.println("document details: " + arangoResult);
                    
                    // get document list
                    test06GetDocumentList(id, rev);
                }
                catch (Exception e) {
                    e.printStackTrace();
                    VertxAssert.fail("test05ReplaceDocument");
                }
                //VertxAssert.testComplete();
            }
        });
    }

    public void test06GetDocumentList(final String id, final String rev) {
        System.out.println("*** test06GetDocumentList ***");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_DOCUMENT);
        requestObject.putString(DocumentAPI.MSG_PROPERTY_ACTION, DocumentAPI.MSG_ACTION_LIST);
        requestObject.putString(DocumentAPI.MSG_PROPERTY_COLLECTION, "testcol");
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("The listing of all the document for the specified collection resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    System.out.println("documents for collection: " + arangoResult);
                    
                    // delete the document
                    test07DeleteDocument(id, rev);
                }
                catch (Exception e) {
                    e.printStackTrace();
                    VertxAssert.fail("test06GetDocumentList");
                }
                //VertxAssert.testComplete();
            }
        });
    }

    public void test07DeleteDocument(final String id, final String rev) {
        System.out.println("*** test07DeleteDocument ***");
        JsonObject requestObject = new JsonObject();
        requestObject.putString(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_DOCUMENT);
        requestObject.putString(DocumentAPI.MSG_PROPERTY_ACTION, DocumentAPI.MSG_ACTION_DELETE);
        requestObject.putString(DocumentAPI.MSG_PROPERTY_ID, id);
        vertx.eventBus().send(address, requestObject, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> reply) {
                try {
                    JsonObject response = reply.body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getObject("result");
                    VertxAssert.assertEquals("The deletion of the specified document resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    //VertxAssert.assertTrue("The request for the specified document was invalid: (returncode: " + arangoResult.getInteger("code") + ")", arangoResult.getInteger("code") == 200);
                    System.out.println("response details: " + arangoResult);
                }
                catch (Exception e) {
                    e.printStackTrace();
                    VertxAssert.fail("test07DeleteDocument");
                }
                VertxAssert.testComplete();
            }
        });
    }

    private JsonObject loadConfig() {
        logger.info(logPrefix + "(re)loading Config");
        URL url = getClass().getResource("/config.json");
        url.getFile();
        Buffer configBuffer = vertx.fileSystem().readFileSync(url.getFile());
        if (configBuffer != null) {
            return new JsonObject(configBuffer.toString());
        }
        
        return new JsonObject();
    }
}
