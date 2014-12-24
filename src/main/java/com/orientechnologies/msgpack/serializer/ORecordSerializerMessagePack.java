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

import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;

/**
 * Created by Enrico Risa on 24/12/14.
 */
public class ORecordSerializerMessagePack implements ORecordSerializer {

  public static final String                       NAME     = "ORecordSerializerMessagePack";

  public static final ORecordSerializerMessagePack INSTANCE = new ORecordSerializerMessagePack();

  @Override
  public ORecord fromStream(byte[] iSource, ORecord iRecord, String[] iFields) {
    return null;
  }

  @Override
  public byte[] toStream(ORecord iSource, boolean iOnlyDelta) {
    return new byte[0];
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
