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

package santo.vertx.arangodb;

import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 *
 * @author sANTo
 */
public class Helper {
    
    private static final String ERROR_FIELD_MISSING = "Required field missing: ";
    
    private static final String PROPERTY_MSG_STATUS = "status";
    private static final String PROPERTY_MSG_STATUSCODE = "statuscode";
    private static final String PROPERTY_MSG_MESSAGE = "message";
    private static final String PROPERTY_MSG_SEVERITY = "severity";
    private static final String PROPERTY_MSG_RESULT = "result";

    private static final String VALUE_MSG_STATUS_OK = "ok";
    private static final String VALUE_MSG_STATUS_ERROR = "error";
    private static final String VALUE_MSG_STATUS_DENIED = "denied";
    
    private static final int VALUE_MSG_STATUSCODE_UNSPECIFIED = 0;

    private static final String VALUE_MSG_SEVERITY_SUCCESS = "success";
    private static final String VALUE_MSG_SEVERITY_INFO = "info";
    private static final String VALUE_MSG_SEVERITY_WARNING = "warning";
    private static final String VALUE_MSG_SEVERITY_DANGER = "danger";

    private static Helper helper = null;
    
    private Helper() {}
    
    public static Helper getHelper() {
        if (helper == null) helper = new Helper();
        return helper;
    }
    
    @Override
    public Object clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException();
    }
    
    public boolean sendSuccess(final Message<JsonObject> msg, final String message) {
        return sendSuccess(msg, VALUE_MSG_STATUSCODE_UNSPECIFIED, message);
    }

    public boolean sendSuccess(final Message<JsonObject> msg, final int statuscode, final String message) {
        return sendSuccess(msg, statuscode, message, VALUE_MSG_SEVERITY_SUCCESS);
    }

    public boolean sendSuccess(final Message<JsonObject> msg, final String message, final String severity) {
        return sendSuccess(msg, VALUE_MSG_STATUSCODE_UNSPECIFIED, message, severity);
    }

    public boolean sendSuccess(final Message<JsonObject> msg, final int statuscode, final String message, final String severity) {
        return sendSuccess(msg, statuscode, message, severity, null);
    }

    public boolean sendSuccess(final Message<JsonObject> msg, final String message, final Object result) {
        return sendSuccess(msg, VALUE_MSG_STATUSCODE_UNSPECIFIED, message, result);
    }

    public boolean sendSuccess(final Message<JsonObject> msg, final int statuscode, final String message, final Object result) {
        return sendSuccess(msg, statuscode, message, VALUE_MSG_SEVERITY_SUCCESS, result);
    }

    public boolean sendSuccess(final Message<JsonObject> msg, final String message, final String severity, final Object result) {
        return sendSuccess(msg, VALUE_MSG_STATUSCODE_UNSPECIFIED, message, severity, result);
    }

    public boolean sendSuccess(final Message<JsonObject> msg, final int statuscode, final String message, final String severity, final Object result) {
        return sendResponse(msg, VALUE_MSG_STATUS_OK, statuscode, message, severity, result);
    }

    public boolean sendError(final Message<JsonObject> msg, final String message) {
        return sendError(msg, VALUE_MSG_STATUSCODE_UNSPECIFIED, message);
    }

    public boolean sendError(final Message<JsonObject> msg, final int statuscode, final String message) {
        return sendError(msg, statuscode, message, VALUE_MSG_SEVERITY_DANGER);
    }

    public boolean sendError(final Message<JsonObject> msg, final String message, final String severity) {
        return sendError(msg, VALUE_MSG_STATUSCODE_UNSPECIFIED, message, severity);
    }

    public boolean sendError(final Message<JsonObject> msg, final int statuscode, final String message, final String severity) {
        return sendError(msg, statuscode, message, severity, null);
    }

    public boolean sendError(final Message<JsonObject> msg, final String message, final Object result) {
        return sendError(msg, VALUE_MSG_STATUSCODE_UNSPECIFIED, message, result);
    }

    public boolean sendError(final Message<JsonObject> msg, final int statuscode, final String message, final Object result) {
        return sendError(msg, statuscode, message, VALUE_MSG_SEVERITY_DANGER, result);
    }

    public boolean sendError(final Message<JsonObject> msg, final String message, final String severity, final Object result) {
        return sendError(msg, VALUE_MSG_STATUSCODE_UNSPECIFIED, message, severity, result);
    }

    public boolean sendError(final Message<JsonObject> msg, final int statuscode, final String message, final String severity, final Object result) {
        return sendResponse(msg, VALUE_MSG_STATUS_ERROR, statuscode, message, severity, result);
    }

    public boolean sendDenied(final Message<JsonObject> msg, final String message) {
        return sendDenied(msg, VALUE_MSG_STATUSCODE_UNSPECIFIED, message);
    }

    public boolean sendDenied(final Message<JsonObject> msg, final int statuscode, final String message) {
        return sendDenied(msg, statuscode, message, VALUE_MSG_SEVERITY_DANGER);
    }
    
    public boolean sendDenied(final Message<JsonObject> msg, final String message, final String severity) {
        return sendDenied(msg, VALUE_MSG_STATUSCODE_UNSPECIFIED, message, severity);
    }

    public boolean sendDenied(final Message<JsonObject> msg, final int statuscode, final String message, final String severity) {
        return sendDenied(msg, statuscode, message, severity, null);
    }

    public boolean sendDenied(final Message<JsonObject> msg, final String message, final Object result) {
        return sendDenied(msg, VALUE_MSG_STATUSCODE_UNSPECIFIED, message, result);
    }

    public boolean sendDenied(final Message<JsonObject> msg, final int statuscode, final String message, final Object result) {
        return sendDenied(msg, statuscode, message, VALUE_MSG_SEVERITY_DANGER, result);
    }

    public boolean sendDenied(final Message<JsonObject> msg, final String message, final String severity, final Object result) {
        return sendDenied(msg, VALUE_MSG_STATUSCODE_UNSPECIFIED, message, severity, result);
    }

    public boolean sendDenied(final Message<JsonObject> msg, final int statuscode, final String message, final String severity, final Object result) {
        return sendResponse(msg, VALUE_MSG_STATUS_DENIED, statuscode, message, severity, result);
    }
    
    public boolean sendResponse(final Message<JsonObject> msg, final String status, final int statuscode, final String message, final String severity, final Object result) {
        boolean success = false;
        
        if (msg == null) {
            success = false;
        }
        else {
            JsonObject responseMsg = new JsonObject();
            responseMsg.put(PROPERTY_MSG_STATUS, status);
            if (statuscode >= 100) responseMsg.put(PROPERTY_MSG_STATUSCODE, statuscode);
            if (message != null) responseMsg.put(PROPERTY_MSG_MESSAGE, message);
            if (severity != null) responseMsg.put(PROPERTY_MSG_SEVERITY, severity);
            if (result != null) responseMsg.put(PROPERTY_MSG_RESULT, result);
            msg.reply(responseMsg);
            success = true;
        }
        
        return success;
    }

    public boolean getOptionalBoolean(JsonObject document, String field) {
        return getBoolean(document, field, null, false, false);
    }

    public boolean getOptionalBoolean(JsonObject document, String field, boolean defaultValue) {
        return getBoolean(document, field, null, defaultValue, false);
    }

    public boolean getMandatoryBoolean(JsonObject document, String field, Message<JsonObject> msg) {
        return getBoolean(document, field, msg, false, false);
    }

    public boolean getMandatoryBoolean(JsonObject document, String field, Message<JsonObject> msg, boolean abort) {
        return getBoolean(document, field, msg, false, abort);
    }

    public boolean getBoolean(JsonObject document, String field, Message<JsonObject> msg, boolean defaultValue, boolean abort) {
        Boolean paramValue = document.getBoolean(field);
        if (paramValue == null) {
            if (abort) throw new IllegalArgumentException(ERROR_FIELD_MISSING + field);
            if (msg != null) sendError(msg, VALUE_MSG_STATUSCODE_UNSPECIFIED, ERROR_FIELD_MISSING + field); // mandatory field
            else paramValue = defaultValue; // optional field
        }

        return paramValue;
    }

    public String getOptionalString(JsonObject document, String field) {
        return getString(document, field, null, null, false);
    }

    public String getOptionalString(JsonObject document, String field, String defaultValue) {
        return getString(document, field, null, defaultValue, false);
    }

    public String getMandatoryString(JsonObject document, String field, Message<JsonObject> msg) {
        return getString(document, field, msg, null, false);
    }

    public String getMandatoryString(JsonObject document, String field, Message<JsonObject> msg, boolean abort) {
        return getString(document, field, msg, null, abort);
    }

    public String getString(JsonObject document, String field, Message<JsonObject> msg, String defaultValue, boolean abort) {
        String paramValue = document.getString(field);
        if (paramValue == null) {
            if (abort) throw new IllegalArgumentException(ERROR_FIELD_MISSING + field);
            if (msg != null) sendError(msg, VALUE_MSG_STATUSCODE_UNSPECIFIED, ERROR_FIELD_MISSING + field); // mandatory field
            else paramValue = defaultValue; // optional field
        }

        return paramValue;
    }

    public int getOptionalInt(JsonObject document, String field, int defaultValue) {
        return getInt(document, field, null, defaultValue, false);
    }

    public int getMandatoryInt(JsonObject document, String field, Message<JsonObject> msg) {
        return getInt(document, field, msg, 0, false);
    }

    public int getMandatoryInt(JsonObject document, String field, Message<JsonObject> msg, boolean abort) {
        return getInt(document, field, msg, 0, abort);
    }

    public int getInt(JsonObject document, String field, Message<JsonObject> msg, int defaultValue, boolean abort) {
        Number paramValue = document.getInteger(field);
        if (paramValue == null) {
            if (abort) throw new IllegalArgumentException(ERROR_FIELD_MISSING + field);
            if (msg != null) sendError(msg, VALUE_MSG_STATUSCODE_UNSPECIFIED, ERROR_FIELD_MISSING + field); // mandatory field
            else paramValue = defaultValue; // optional field
        }

        return paramValue.intValue();
    }

    public long getOptionalLong(JsonObject document, String field, long defaultValue) {
        return getLong(document, field, null, defaultValue, false);
    }

    public long getMandatoryLong(JsonObject document, String field, Message<JsonObject> msg) {
        return getLong(document, field, msg, 0, false);
    }

    public long getMandatoryLong(JsonObject document, String field, Message<JsonObject> msg, boolean abort) {
        return getLong(document, field, msg, 0, abort);
    }

    public long getLong(JsonObject document, String field, Message<JsonObject> msg, long defaultValue, boolean abort) {
        Number paramValue = document.getInteger(field);
        if (paramValue == null) {
            if (abort) throw new IllegalArgumentException(ERROR_FIELD_MISSING + field);
            if (msg != null) sendError(msg, VALUE_MSG_STATUSCODE_UNSPECIFIED, ERROR_FIELD_MISSING + field); // mandatory field
            else paramValue = defaultValue; // optional field
        }

        return paramValue.longValue();
    }

    public JsonObject getOptionalObject(JsonObject document, String field) {
        return getObject(document, field, null, null, false);
    }

    public JsonObject getOptionalObject(JsonObject document, String field, JsonObject defaultValue) {
        return getObject(document, field, null, defaultValue, false);
    }

    public JsonObject getMandatoryObject(JsonObject document, String field, Message<JsonObject> msg) {
        return getObject(document, field, msg, null, false);
    }

    public JsonObject getMandatoryObject(JsonObject document, String field, Message<JsonObject> msg, boolean abort) {
        return getObject(document, field, msg, null, abort);
    }

    public JsonObject getObject(JsonObject document, String field, Message<JsonObject> msg, JsonObject defaultValue, boolean abort) {
        JsonObject paramValue = document.getJsonObject(field);
        if (paramValue == null) {
            if (abort) throw new IllegalArgumentException(ERROR_FIELD_MISSING + field);
            if (msg != null) sendError(msg, VALUE_MSG_STATUSCODE_UNSPECIFIED, ERROR_FIELD_MISSING + field); // mandatory field
            else paramValue = defaultValue; // optional field
        }

        return paramValue;
    }

    public JsonArray getOptionalArray(JsonObject document, String field) {
        return getArray(document, field, null, null, false);
    }

    public JsonArray getOptionalArray(JsonObject document, String field, JsonArray defaultValue) {
        return getArray(document, field, null, defaultValue, false);
    }

    public JsonArray getMandatoryArray(JsonObject document, String field, Message<JsonObject> msg) {
        return getArray(document, field, msg, null, false);
    }

    public JsonArray getMandatoryArray(JsonObject document, String field, Message<JsonObject> msg, boolean abort) {
        return getArray(document, field, msg, null, abort);
    }

    public JsonArray getArray(JsonObject document, String field, Message<JsonObject> msg, JsonArray defaultValue, boolean abort) {
        JsonArray paramValue = document.getJsonArray(field);
        if (paramValue == null) {
            if (abort) throw new IllegalArgumentException(ERROR_FIELD_MISSING + field);
            if (msg != null) sendError(msg, VALUE_MSG_STATUSCODE_UNSPECIFIED, ERROR_FIELD_MISSING + field); // mandatory field
            else paramValue = defaultValue; // optional field
        }

        return paramValue;
    }

}
