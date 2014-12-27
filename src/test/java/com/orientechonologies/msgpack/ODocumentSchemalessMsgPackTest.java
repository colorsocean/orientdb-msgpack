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

package com.orientechonologies.msgpack;

import com.orientechnologies.msgpack.serializer.ORecordSerializerMessagePack;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Calendar;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 * Created by Enrico Risa on 24/12/14.
 */
public class ODocumentSchemalessMsgPackTest {

    protected ORecordSerializerMessagePack serializer;

    public ODocumentSchemalessMsgPackTest() {
        serializer = new ORecordSerializerMessagePack();
    }

    @Test
    public void testSimpleDocument() {

        ODocument document = new ODocument();

        document.field("name", "name");
        document.field("age", 20);
        document.field("youngAge", (short) 20);
        document.field("oldAge", (long) 20);
        document.field("heigth", 12.5f);
        document.field("bitHeigth", 12.5d);
        document.field("class", (byte) 'C');
        document.field("nullField", (Object) null);
        document.field("character", 'C');
        document.field("alive", true);
//        document.field("dateTime", new Date());
//        document.field("bigNumber", new BigDecimal("43989872423376487952454365232141525434.32146432321442534"));
        Calendar c = Calendar.getInstance();
//      document.field("date", c.getTime(), OType.DATE);
        Calendar c1 = Calendar.getInstance();
        c1.set(Calendar.MILLISECOND, 0);
        c1.set(Calendar.SECOND, 0);
        c1.set(Calendar.MINUTE, 0);
        c1.set(Calendar.HOUR_OF_DAY, 0);
//      document.field("date1", c1.getTime(), OType.DATE);

        byte[] byteValue = new byte[10];
        Arrays.fill(byteValue, (byte) 10);
        document.field("bytes", byteValue);

        document.field("utf8String", new String("A" + "\u00ea" + "\u00f1" + "\u00fc" + "C"));
//      document.field("recordId", new ORecordId(10, 10));

        byte[] res = serializer.toStream(document, false);
        ODocument extr = (ODocument) serializer.fromStream(res, new ODocument(), new String[]{});

        c.set(Calendar.MILLISECOND, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.HOUR_OF_DAY, 0);

        assertEquals(extr.fields(), document.fields());
        assertEquals(extr.field("name"), document.field("name"));
        assertEquals(extr.field("age"), document.field("age"));
        assertEquals(extr.field("youngAge"), document.field("youngAge"));
        assertEquals(extr.field("oldAge"), document.field("oldAge"));
        assertEquals(extr.field("heigth"), document.field("heigth"));
        assertEquals(extr.field("bitHeigth"), document.field("bitHeigth"));
        assertEquals(extr.field("class"), document.field("class"));
        // TODO fix char management issue:#2427
        // assertEquals(document.field("character"), extr.field("character"));
        assertEquals(extr.field("alive"), document.field("alive"));
        assertEquals(extr.field("dateTime"), document.field("dateTime"));
        assertEquals(extr.field("date"), c.getTime());
        assertEquals(extr.field("date1"), c1.getTime());
        assertEquals(extr.field("bytes"), document.field("bytes"));
        assertEquals(extr.field("utf8String"), document.field("utf8String"));
        assertEquals(extr.field("recordId"), document.field("recordId"));
        assertEquals(extr.field("bigNumber"), document.field("bigNumber"));
        assertNull(extr.field("nullField"));
    }
}
