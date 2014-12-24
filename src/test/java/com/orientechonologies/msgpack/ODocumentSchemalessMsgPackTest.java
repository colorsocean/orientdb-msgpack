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

import com.orientechnologies.orient.core.record.impl.ODocument;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.msgpack.serializer.ORecordSerializerMessagePack;

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

    ODocument doc = new ODocument();
    doc.field("name", "Mike");

    byte[] bytes = serializer.toStream(doc, false);

    ODocument document = new ODocument();
    serializer.fromStream(bytes, document, new String[] {});

    Assert.assertEquals(document.field("name"), doc.field("name"));
  }
}
