/*
 *
 *  * Copyright 2014 Orient Technologies.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.orientechnologies.msgpack.serializer;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import org.msgpack.core.MessageFormat;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.holder.ValueHolder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Enrico Risa on 24/12/14.
 */
public class ORecordSerializerMessagePack implements ORecordSerializer {

    public static final String NAME = "ORecordSerializerMessagePack";

    public static final ORecordSerializerMessagePack INSTANCE = new ORecordSerializerMessagePack();

    @Override
    public ORecord fromStream(byte[] iSource, ORecord iRecord, String[] iFields) {

        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(iSource);

        try {
            deserializeDocument(unpacker, (ODocument) iRecord);
        } catch (IOException e) {
            OLogManager.instance().error(this, "Error deserializing");
        }
        return iRecord;
    }

    @Override
    public byte[] toStream(ORecord iSource, boolean iOnlyDelta) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        MessagePacker packer = MessagePack.newDefaultPacker(out);
        try {
            serializeClass(iSource, packer);
            serializeDocument((ODocument) iSource, packer);
            packer.close();
            return out.toByteArray();
        } catch (IOException e) {
            OLogManager.instance().error(this, "Error serializing document");
            throw new RuntimeException(e);
        }
    }

    /* Serializer */
    private void serializeClass(ORecord iSource, MessagePacker packer) throws IOException {
        ODocument doc = (ODocument) iSource;
        String className = doc.getClassName();
        packer.packString(className != null ? className : "");
    }

    private void serializeDocument(ODocument document, MessagePacker packer) throws IOException {

        Map<String, OProperty> properties = new HashMap<>();
        for (Map.Entry<String, Object> entry : document) {
            OType type = getFieldType(document, entry.getKey(), entry.getValue(), properties);

            writeSingleValue(packer, entry.getKey(), entry.getValue(), type);
        }
    }

    private void writeSingleValue(MessagePacker packer, String key, Object value, OType type) throws IOException {

        packer.packString(key);

        if (type == null || value == null) {
            packer.packNil();
            return;
        }

        switch (type) {
            case INTEGER:

                packer.packInt(((Number) value).intValue());
                break;
            case LONG:
                packer.packLong(((Number) value).longValue());
                break;
            case SHORT:
                packer.packShort(((Number) value).shortValue());
                break;
            case STRING:
                packer.packString("" + value);
                break;
            case DOUBLE:
                packer.packDouble(((Number) value).doubleValue());
                break;
            case FLOAT:
                packer.packFloat(((Number) value).floatValue());
                break;
            case BYTE:
                packer.packByte((Byte) value);
                break;
            case BOOLEAN:
                packer.packBoolean((Boolean) value);
                break;
            case DATETIME:

                break;
            case DATE:

                break;
            case EMBEDDED:
                break;
            case EMBEDDEDSET:
            case EMBEDDEDLIST:
                break;
            case DECIMAL:
                BigDecimal decimalValue = (BigDecimal) value;
                packer.packBigInteger(decimalValue.unscaledValue());
                break;
            case BINARY:
                byte[] bytes = (byte[]) value;
                packer.packBinaryHeader(bytes.length);
                packer.writePayload(bytes);
                break;
            case LINKSET:
            case LINKLIST:
                break;
            case LINK:
                break;
            case LINKMAP:
                break;
            case EMBEDDEDMAP:
                break;
            case LINKBAG:
                break;
            case CUSTOM:
                break;
            case TRANSIENT:
                break;
            case ANY:
                break;
        }
    }

    private OType getFieldType(final ODocument document, final String key, final Object fieldValue,
                               final Map<String, OProperty> properties) {
        OType type = document.fieldType(key);
        if (type == null) {
            final OProperty prop = properties != null ? properties.get(key) : null;
            if (prop != null)
                type = prop.getType();

            if (type == null || OType.ANY == type)
                type = OType.getTypeByValue(fieldValue);
        }
        return type;
    }

    private void deserializeDocument(MessageUnpacker unpacker, ODocument document) throws IOException {

        String className = unpacker.unpackString();
        if (className != null && !className.isEmpty())
            ODocumentInternal.fillClassNameIfNeeded(document, className);

        String fieldName = null;
        Object value = null;
        ValueHolder v = null;
        MessageFormat format = null;
        while (unpacker.hasNext()) {

            v = new ValueHolder();
            unpacker.unpackValue(v);
            fieldName = v.get().asString().toString();
            v = new ValueHolder();
            format = unpacker.unpackValue(v);
            switch (format.getValueType()) {

                case INTEGER:
                    value = v.get().asInteger().toInt();
                    break;
                case BOOLEAN:
                    value = v.get().asBoolean().toBoolean();
                    break;
                case FLOAT:
                    value = v.get().asFloat().toFloat();
                    break;
                case STRING:
                    value = v.get().asString().toString();
                    break;

                case BINARY:
                    value = v.get().asBinary().toByteArray();

            }

            document.field(fieldName, value);
        }
    }

    @Override
    public int getCurrentVersion() {
        return 0;
    }

    @Override
    public int getMinSupportedVersion() {
        return 0;
    }
}
