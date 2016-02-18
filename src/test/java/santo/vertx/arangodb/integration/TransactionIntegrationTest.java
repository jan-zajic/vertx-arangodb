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
import santo.vertx.arangodb.rest.TransactionAPI;
/**
 * Integration tests for the {@link santo.vertx.arangodb.rest.TransactionAPI} against an external <a href="http://www.arangodb.com">ArangoDB</a> instance
 * 
 * @author sANTo
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TransactionIntegrationTest extends BaseIntegrationTest {
        
    @Test
    public void test01ExecuteSimpleTransaction(TestContext context) {
        System.out.println("*** test01ExecuteSimpleTransaction ***");
        JsonObject transactionObject = new JsonObject();
        JsonObject collectionsObject = new JsonObject();
        collectionsObject.put("write", vertexColName);
        transactionObject.put(TransactionAPI.DOC_ATTRIBUTE_COLLECTIONS, collectionsObject);
        StringBuilder action = new StringBuilder();
        action.append("function () {");
        action.append("var db = require('internal').db;");
        action.append("db.").append(vertexColName).append(".save({'description': 'transaction doc'});");
        action.append("return db.").append(vertexColName).append(".count();");
        action.append("}");
        transactionObject.put(TransactionAPI.DOC_ATTRIBUTE_ACTION, action.toString());
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_TRANSACTION);
        requestObject.put(TransactionAPI.MSG_PROPERTY_ACTION, TransactionAPI.MSG_ACTION_EXECUTE);
        requestObject.put(TransactionAPI.MSG_PROPERTY_DOCUMENT, transactionObject);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("Transaction operation resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    context.assertTrue(!arangoResult.getBoolean("error"), "Transaction operation resulted in an error: " + arangoResult.getString("errorMessage"));
                    context.assertTrue(arangoResult.getInteger("code") == 200, "wrong returncode received: " + arangoResult.getInteger("code"));
                }
                catch (Exception e) {
                    context.fail("test01ExecuteSimpleTransaction");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test02ExecuteInvalidTransaction(TestContext context) {
        System.out.println("*** test02ExecuteInvalidTransaction ***");
        JsonObject transactionObject = new JsonObject();
        JsonObject collectionsObject = new JsonObject();
        collectionsObject.put("write", "invalid-collection");
        transactionObject.put(TransactionAPI.DOC_ATTRIBUTE_COLLECTIONS, collectionsObject);
        StringBuilder action = new StringBuilder();
        action.append("function () {");
        action.append("var db = require('internal').db;");
        action.append("db.").append("invalid-collection").append(".save({'description': 'transaction doc'});");
        action.append("return db.").append("invalid-collection").append(".count();");
        action.append("}");
        transactionObject.put(TransactionAPI.DOC_ATTRIBUTE_ACTION, action.toString());
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_TRANSACTION);
        requestObject.put(TransactionAPI.MSG_PROPERTY_ACTION, TransactionAPI.MSG_ACTION_EXECUTE);
        requestObject.put(TransactionAPI.MSG_PROPERTY_DOCUMENT, transactionObject);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("Transaction operation didn't return the expected error: " + response.getString("status"), "error", response.getString("status"));
                    context.assertTrue(arangoResult.getBoolean("error"), "Transaction operation didn't return the expected error: " + arangoResult.getBoolean("error"));
                    context.assertTrue(arangoResult.getInteger("code") == 404, "wrong returncode received: " + arangoResult.getInteger("code"));
                }
                catch (Exception e) {
                    context.fail("test02ExecuteInvalidTransaction");
                }
                async.complete();
            }
        });
    }

}
