// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.
// http://code.google.com/p/protobuf/
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Note:  This file contains many lines over 80 characters.  It even contains
// many lines over 100 characters, which fails a presubmit test.  However,
// given the extremely repetitive nature of the file, I (kenton) feel that
// having similar components of each statement line up is more important than
// avoiding horizontal scrolling.  So, I am bypassing the presubmit check.

package com.google.protobuf;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import junit.framework.Assert;

import org.apache.activemq.protobuf.Buffer;

import protobuf_unittest.UnittestProto.ForeignEnum;
import protobuf_unittest.UnittestProto.ForeignMessage;
import protobuf_unittest.UnittestProto.TestAllTypes;

import com.google.protobuf.test.UnittestImport.ImportEnum;
import com.google.protobuf.test.UnittestImport.ImportMessage;

/**
 * Contains methods for setting all fields of {@code TestAllTypes} to
 * some vaules as well as checking that all the fields are set to those values.
 * These are useful for testing various protocol message features, e.g.
 * set all fields of a message, serialize it, parse it, and check that all
 * fields are set.
 *
 * @author kenton@google.com Kenton Varda
 */
class TestUtil {
  private TestUtil() {}

  /** Helper to convert a String to ByteSequence. */
  private static Buffer toBytes(String str) {
    try {
      return new Buffer(str.getBytes("UTF-8"));
    } catch(java.io.UnsupportedEncodingException e) {
      throw new RuntimeException("UTF-8 not supported.", e);
    }
  }

  /**
   * Get a {@code TestAllTypes} with all fields set as they would be by
   * {@link #setAllFields(TestAllTypes.Builder)}.
   */
  public static TestAllTypes getAllSet() {
    TestAllTypes builder = new TestAllTypes();
    setAllFields(builder);
    return builder;
  }

  /**
   * Set every field of {@code message} to the values expected by
   * {@code assertAllFieldsSet()}.
   */
  public static void setAllFields(protobuf_unittest.UnittestProto.TestAllTypes message) {
    message.setOptionalInt32   (101);
    message.setOptionalInt64   (102);
    message.setOptionalUint32  (103);
    message.setOptionalUint64  (104);
    message.setOptionalSint32  (105);
    message.setOptionalSint64  (106);
    message.setOptionalFixed32 (107);
    message.setOptionalFixed64 (108);
    message.setOptionalSfixed32(109);
    message.setOptionalSfixed64(110);
    message.setOptionalFloat   (111);
    message.setOptionalDouble  (112);
    message.setOptionalBool    (true);
    message.setOptionalString  ("115");
    message.setOptionalBytes   (toBytes("116"));

    message.setOptionalGroup(
      new TestAllTypes.OptionalGroup().setA(117));
    message.setOptionalNestedMessage(
      new TestAllTypes.NestedMessage().setBb(118));
    message.setOptionalForeignMessage(
      new ForeignMessage().setC(119));
    message.setOptionalImportMessage(
      new ImportMessage().setD(120));

    message.setOptionalNestedEnum (TestAllTypes.NestedEnum.BAZ);
    message.setOptionalForeignEnum(ForeignEnum.FOREIGN_BAZ);
    message.setOptionalImportEnum (ImportEnum.IMPORT_BAZ);

    message.setOptionalStringPiece("124");
    message.setOptionalCord("125");

    // -----------------------------------------------------------------

    message.getRepeatedInt32List().add(201);
    message.getRepeatedInt64List().add(202L);
    message.getRepeatedUint32List().add(203);
    message.getRepeatedUint64List().add(204l);
    message.getRepeatedSint32List().add(205);
    message.getRepeatedSint64List().add  (206l);
    message.getRepeatedFixed32List().add (207);
    message.getRepeatedFixed64List().add (208l);
    message.getRepeatedSfixed32List().add(209);
    message.getRepeatedSfixed64List().add(210l);
    message.getRepeatedFloatList().add   (211f);
    message.getRepeatedDoubleList().add  (212d);
    message.getRepeatedBoolList().add    (true);
    message.getRepeatedStringList().add  ("215");
    message.getRepeatedBytesList().add   (toBytes("216"));

    message.getRepeatedGroupList().add(
      new TestAllTypes.RepeatedGroup().setA(217));
    message.getRepeatedNestedMessageList().add(
      new TestAllTypes.NestedMessage().setBb(218));
    message.getRepeatedForeignMessageList().add(
      new ForeignMessage().setC(219));
    message.getRepeatedImportMessageList().add(
      new ImportMessage().setD(220));

    message.getRepeatedNestedEnumList().add(TestAllTypes.NestedEnum.BAR);
    message.getRepeatedForeignEnumList().add(ForeignEnum.FOREIGN_BAR);
    message.getRepeatedImportEnumList().add(ImportEnum.IMPORT_BAR);

    message.getRepeatedStringPieceList().add("224");
    message.getRepeatedCordList().add("225");

    // Add a second one of each field.
    message.getRepeatedInt32List().add(301);
    message.getRepeatedInt64List().add(302L);
    message.getRepeatedUint32List().add(303);
    message.getRepeatedUint64List().add(304l);
    message.getRepeatedSint32List().add(305);
    message.getRepeatedSint64List().add  (306l);
    message.getRepeatedFixed32List().add (307);
    message.getRepeatedFixed64List().add (308l);
    message.getRepeatedSfixed32List().add(309);
    message.getRepeatedSfixed64List().add(310l);
    message.getRepeatedFloatList().add   (311f);
    message.getRepeatedDoubleList().add  (312d);
    message.getRepeatedBoolList().add    (false);
    message.getRepeatedStringList().add  ("315");
    message.getRepeatedBytesList().add   (toBytes("316"));

    message.getRepeatedGroupList().add(
      new TestAllTypes.RepeatedGroup().setA(317));
    message.getRepeatedNestedMessageList().add(
      new TestAllTypes.NestedMessage().setBb(318));
    message.getRepeatedForeignMessageList().add(
      new ForeignMessage().setC(319));
    message.getRepeatedImportMessageList().add(
      new ImportMessage().setD(320));

    message.getRepeatedNestedEnumList().add(TestAllTypes.NestedEnum.BAZ);
    message.getRepeatedForeignEnumList().add(ForeignEnum.FOREIGN_BAZ);
    message.getRepeatedImportEnumList().add(ImportEnum.IMPORT_BAZ);

    message.getRepeatedStringPieceList().add("324");
    message.getRepeatedCordList().add("325");


    // -----------------------------------------------------------------

    message.setDefaultInt32   (401);
    message.setDefaultInt64   (402);
    message.setDefaultUint32  (403);
    message.setDefaultUint64  (404);
    message.setDefaultSint32  (405);
    message.setDefaultSint64  (406);
    message.setDefaultFixed32 (407);
    message.setDefaultFixed64 (408);
    message.setDefaultSfixed32(409);
    message.setDefaultSfixed64(410);
    message.setDefaultFloat   (411);
    message.setDefaultDouble  (412);
    message.setDefaultBool    (false);
    message.setDefaultString  ("415");
    message.setDefaultBytes   (toBytes("416"));

    message.setDefaultNestedEnum (TestAllTypes.NestedEnum.FOO);
    message.setDefaultForeignEnum(ForeignEnum.FOREIGN_FOO);
    message.setDefaultImportEnum (ImportEnum.IMPORT_FOO);

    message.setDefaultStringPiece("424");
    message.setDefaultCord("425");
  }

  // -------------------------------------------------------------------

  /**
   * Modify the repeated fields of {@code message} to contain the values
   * expected by {@code assertRepeatedFieldsModified()}.
   */
  public static void modifyRepeatedFields(TestAllTypes message) {
    message.getRepeatedInt32List().set(1, 501);
    message.getRepeatedInt64List().set   (1, 502l);
    message.getRepeatedUint32List().set  (1, 503);
    message.getRepeatedUint64List().set  (1, 504l);
    message.getRepeatedSint32List().set  (1, 505);
    message.getRepeatedSint64List().set  (1, 506l);
    message.getRepeatedFixed32List().set (1, 507);
    message.getRepeatedFixed64List().set (1, 508l);
    message.getRepeatedSfixed32List().set(1, 509);
    message.getRepeatedSfixed64List().set(1, 510l);
    message.getRepeatedFloatList().set   (1, 511f);
    message.getRepeatedDoubleList().set  (1, 512d);
    message.getRepeatedBoolList().set    (1, true);
    message.getRepeatedStringList().set  (1, "515");
    message.getRepeatedBytesList().set   (1, toBytes("516"));

    message.getRepeatedGroupList().set(1,
      new TestAllTypes.RepeatedGroup().setA(517));
    message.getRepeatedNestedMessageList().set(1,
      new TestAllTypes.NestedMessage().setBb(518));
    message.getRepeatedForeignMessageList().set(1,
      new ForeignMessage().setC(519));
    message.getRepeatedImportMessageList().set(1,
      new ImportMessage().setD(520));

    message.getRepeatedNestedEnumList().set (1, TestAllTypes.NestedEnum.FOO);
    message.getRepeatedForeignEnumList().set(1, ForeignEnum.FOREIGN_FOO);
    message.getRepeatedImportEnumList().set (1, ImportEnum.IMPORT_FOO);

    message.getRepeatedStringPieceList().set(1, "524");
    message.getRepeatedCordList().set(1, "525");
  }

  // -------------------------------------------------------------------

  /**
   * Assert (using {@code junit.framework.Assert}} that all fields of
   * {@code message} are set to the values assigned by {@code setAllFields}.
   */
  public static void assertAllFieldsSet(TestAllTypes message) {
    Assert.assertTrue(message.hasOptionalInt32   ());
    Assert.assertTrue(message.hasOptionalInt64   ());
    Assert.assertTrue(message.hasOptionalUint32  ());
    Assert.assertTrue(message.hasOptionalUint64  ());
    Assert.assertTrue(message.hasOptionalSint32  ());
    Assert.assertTrue(message.hasOptionalSint64  ());
    Assert.assertTrue(message.hasOptionalFixed32 ());
    Assert.assertTrue(message.hasOptionalFixed64 ());
    Assert.assertTrue(message.hasOptionalSfixed32());
    Assert.assertTrue(message.hasOptionalSfixed64());
    Assert.assertTrue(message.hasOptionalFloat   ());
    Assert.assertTrue(message.hasOptionalDouble  ());
    Assert.assertTrue(message.hasOptionalBool    ());
    Assert.assertTrue(message.hasOptionalString  ());
    Assert.assertTrue(message.hasOptionalBytes   ());

    Assert.assertTrue(message.hasOptionalGroup         ());
    Assert.assertTrue(message.hasOptionalNestedMessage ());
    Assert.assertTrue(message.hasOptionalForeignMessage());
    Assert.assertTrue(message.hasOptionalImportMessage ());

    Assert.assertTrue(message.getOptionalGroup         ().hasA());
    Assert.assertTrue(message.getOptionalNestedMessage ().hasBb());
    Assert.assertTrue(message.getOptionalForeignMessage().hasC());
    Assert.assertTrue(message.getOptionalImportMessage ().hasD());

    Assert.assertTrue(message.hasOptionalNestedEnum ());
    Assert.assertTrue(message.hasOptionalForeignEnum());
    Assert.assertTrue(message.hasOptionalImportEnum ());

    Assert.assertTrue(message.hasOptionalStringPiece());
    Assert.assertTrue(message.hasOptionalCord());

    Assert.assertEquals(101  , message.getOptionalInt32   ());
    Assert.assertEquals(102  , message.getOptionalInt64   ());
    Assert.assertEquals(103  , message.getOptionalUint32  ());
    Assert.assertEquals(104  , message.getOptionalUint64  ());
    Assert.assertEquals(105  , message.getOptionalSint32  ());
    Assert.assertEquals(106  , message.getOptionalSint64  ());
    Assert.assertEquals(107  , message.getOptionalFixed32 ());
    Assert.assertEquals(108  , message.getOptionalFixed64 ());
    Assert.assertEquals(109  , message.getOptionalSfixed32());
    Assert.assertEquals(110  , message.getOptionalSfixed64());
    Assert.assertEquals(111  , message.getOptionalFloat   (), 0.0);
    Assert.assertEquals(112  , message.getOptionalDouble  (), 0.0);
    Assert.assertEquals(true , message.getOptionalBool    ());
    Assert.assertEquals("115", message.getOptionalString  ());
    Assert.assertEquals(toBytes("116"), message.getOptionalBytes());

    Assert.assertEquals(117, message.getOptionalGroup         ().getA());
    Assert.assertEquals(118, message.getOptionalNestedMessage ().getBb());
    Assert.assertEquals(119, message.getOptionalForeignMessage().getC());
    Assert.assertEquals(120, message.getOptionalImportMessage ().getD());

    Assert.assertEquals(TestAllTypes.NestedEnum.BAZ, message.getOptionalNestedEnum());
    Assert.assertEquals(ForeignEnum.FOREIGN_BAZ, message.getOptionalForeignEnum());
    Assert.assertEquals(ImportEnum.IMPORT_BAZ, message.getOptionalImportEnum());

    Assert.assertEquals("124", message.getOptionalStringPiece());
    Assert.assertEquals("125", message.getOptionalCord());

    // -----------------------------------------------------------------

    Assert.assertEquals(2, message.getRepeatedInt32List().size   ());
    Assert.assertEquals(2, message.getRepeatedInt64List().size   ());
    Assert.assertEquals(2, message.getRepeatedUint32List().size  ());
    Assert.assertEquals(2, message.getRepeatedUint64List().size  ());
    Assert.assertEquals(2, message.getRepeatedSint32List().size  ());
    Assert.assertEquals(2, message.getRepeatedSint64List().size  ());
    Assert.assertEquals(2, message.getRepeatedFixed32List().size ());
    Assert.assertEquals(2, message.getRepeatedFixed64List().size ());
    Assert.assertEquals(2, message.getRepeatedSfixed32List().size());
    Assert.assertEquals(2, message.getRepeatedSfixed64List().size());
    Assert.assertEquals(2, message.getRepeatedFloatList().size   ());
    Assert.assertEquals(2, message.getRepeatedDoubleList().size  ());
    Assert.assertEquals(2, message.getRepeatedBoolList().size    ());
    Assert.assertEquals(2, message.getRepeatedStringList().size  ());
    Assert.assertEquals(2, message.getRepeatedBytesList().size   ());

    Assert.assertEquals(2, message.getRepeatedGroupList().size         ());
    Assert.assertEquals(2, message.getRepeatedNestedMessageList().size ());
    Assert.assertEquals(2, message.getRepeatedForeignMessageList().size());
    Assert.assertEquals(2, message.getRepeatedImportMessageList().size ());
    Assert.assertEquals(2, message.getRepeatedNestedEnumList().size    ());
    Assert.assertEquals(2, message.getRepeatedForeignEnumList().size   ());
    Assert.assertEquals(2, message.getRepeatedImportEnumList().size    ());

    Assert.assertEquals(2, message.getRepeatedStringPieceList().size());
    Assert.assertEquals(2, message.getRepeatedCordList().size());

    Assert.assertEquals(201  , (int)message.getRepeatedInt32List().get(0));
    Assert.assertEquals(202  , (long)message.getRepeatedInt64List().get   (0));
    Assert.assertEquals(203  , (int)message.getRepeatedUint32List().get  (0));
    Assert.assertEquals(204  , (long)message.getRepeatedUint64List().get  (0));
    Assert.assertEquals(205  , (int)message.getRepeatedSint32List().get  (0));
    Assert.assertEquals(206  , (long)message.getRepeatedSint64List().get  (0));
    Assert.assertEquals(207  , (int)message.getRepeatedFixed32List().get (0));
    Assert.assertEquals(208  , (long)message.getRepeatedFixed64List().get (0));
    Assert.assertEquals(209  , (int)message.getRepeatedSfixed32List().get(0));
    Assert.assertEquals(210  , (long)message.getRepeatedSfixed64List().get(0));
    Assert.assertEquals(211  , message.getRepeatedFloatList().get   (0), 0.0);
    Assert.assertEquals(212  , message.getRepeatedDoubleList().get  (0), 0.0);
    Assert.assertEquals(true , (boolean)message.getRepeatedBoolList().get    (0));
    Assert.assertEquals("215", message.getRepeatedStringList().get  (0));
    Assert.assertEquals(toBytes("216"), message.getRepeatedBytesList().get(0));

    Assert.assertEquals(217, message.getRepeatedGroupList().get         (0).getA());
    Assert.assertEquals(218, message.getRepeatedNestedMessageList().get (0).getBb());
    Assert.assertEquals(219, message.getRepeatedForeignMessageList().get(0).getC());
    Assert.assertEquals(220, message.getRepeatedImportMessageList().get (0).getD());

    Assert.assertEquals(TestAllTypes.NestedEnum.BAR, message.getRepeatedNestedEnumList().get (0));
    Assert.assertEquals(ForeignEnum.FOREIGN_BAR, message.getRepeatedForeignEnumList().get(0));
    Assert.assertEquals(ImportEnum.IMPORT_BAR, message.getRepeatedImportEnumList().get(0));

    Assert.assertEquals("224", message.getRepeatedStringPieceList().get(0));
    Assert.assertEquals("225", message.getRepeatedCordList().get(0));

    Assert.assertEquals(301  , (int)message.getRepeatedInt32List().get   (1));
    Assert.assertEquals(302  , (long)message.getRepeatedInt64List().get   (1));
    Assert.assertEquals(303  , (int)message.getRepeatedUint32List().get  (1));
    Assert.assertEquals(304  , (long)message.getRepeatedUint64List().get  (1));
    Assert.assertEquals(305  , (int)message.getRepeatedSint32List().get  (1));
    Assert.assertEquals(306  , (long)message.getRepeatedSint64List().get  (1));
    Assert.assertEquals(307  , (int)message.getRepeatedFixed32List().get (1));
    Assert.assertEquals(308  , (long)message.getRepeatedFixed64List().get (1));
    Assert.assertEquals(309  , (int)message.getRepeatedSfixed32List().get(1));
    Assert.assertEquals(310  , (long)message.getRepeatedSfixed64List().get(1));
    Assert.assertEquals(311  , message.getRepeatedFloatList().get   (1), 0.0);
    Assert.assertEquals(312  , message.getRepeatedDoubleList().get  (1), 0.0);
    Assert.assertEquals(false, (boolean)message.getRepeatedBoolList().get    (1));
    Assert.assertEquals("315", message.getRepeatedStringList().get  (1));
    Assert.assertEquals(toBytes("316"), message.getRepeatedBytesList().get(1));

    Assert.assertEquals(317, message.getRepeatedGroupList().get         (1).getA());
    Assert.assertEquals(318, message.getRepeatedNestedMessageList().get (1).getBb());
    Assert.assertEquals(319, message.getRepeatedForeignMessageList().get(1).getC());
    Assert.assertEquals(320, message.getRepeatedImportMessageList().get (1).getD());

    Assert.assertEquals(TestAllTypes.NestedEnum.BAZ, message.getRepeatedNestedEnumList().get (1));
    Assert.assertEquals(ForeignEnum.FOREIGN_BAZ, message.getRepeatedForeignEnumList().get(1));
    Assert.assertEquals(ImportEnum.IMPORT_BAZ, message.getRepeatedImportEnumList().get(1));

    Assert.assertEquals("324", message.getRepeatedStringPieceList().get(1));
    Assert.assertEquals("325", message.getRepeatedCordList().get(1));

    // -----------------------------------------------------------------

    Assert.assertTrue(message.hasDefaultInt32   ());
    Assert.assertTrue(message.hasDefaultInt64   ());
    Assert.assertTrue(message.hasDefaultUint32  ());
    Assert.assertTrue(message.hasDefaultUint64  ());
    Assert.assertTrue(message.hasDefaultSint32  ());
    Assert.assertTrue(message.hasDefaultSint64  ());
    Assert.assertTrue(message.hasDefaultFixed32 ());
    Assert.assertTrue(message.hasDefaultFixed64 ());
    Assert.assertTrue(message.hasDefaultSfixed32());
    Assert.assertTrue(message.hasDefaultSfixed64());
    Assert.assertTrue(message.hasDefaultFloat   ());
    Assert.assertTrue(message.hasDefaultDouble  ());
    Assert.assertTrue(message.hasDefaultBool    ());
    Assert.assertTrue(message.hasDefaultString  ());
    Assert.assertTrue(message.hasDefaultBytes   ());

    Assert.assertTrue(message.hasDefaultNestedEnum ());
    Assert.assertTrue(message.hasDefaultForeignEnum());
    Assert.assertTrue(message.hasDefaultImportEnum ());

    Assert.assertTrue(message.hasDefaultStringPiece());
    Assert.assertTrue(message.hasDefaultCord());

    Assert.assertEquals(401  , message.getDefaultInt32   ());
    Assert.assertEquals(402  , message.getDefaultInt64   ());
    Assert.assertEquals(403  , message.getDefaultUint32  ());
    Assert.assertEquals(404  , message.getDefaultUint64  ());
    Assert.assertEquals(405  , message.getDefaultSint32  ());
    Assert.assertEquals(406  , message.getDefaultSint64  ());
    Assert.assertEquals(407  , message.getDefaultFixed32 ());
    Assert.assertEquals(408  , message.getDefaultFixed64 ());
    Assert.assertEquals(409  , message.getDefaultSfixed32());
    Assert.assertEquals(410  , message.getDefaultSfixed64());
    Assert.assertEquals(411  , message.getDefaultFloat   (), 0.0);
    Assert.assertEquals(412  , message.getDefaultDouble  (), 0.0);
    Assert.assertEquals(false, message.getDefaultBool    ());
    Assert.assertEquals("415", message.getDefaultString  ());
    Assert.assertEquals(toBytes("416"), message.getDefaultBytes());

    Assert.assertEquals(TestAllTypes.NestedEnum.FOO, message.getDefaultNestedEnum ());
    Assert.assertEquals(ForeignEnum.FOREIGN_FOO, message.getDefaultForeignEnum());
    Assert.assertEquals(ImportEnum.IMPORT_FOO, message.getDefaultImportEnum());

    Assert.assertEquals("424", message.getDefaultStringPiece());
    Assert.assertEquals("425", message.getDefaultCord());
  }

  // -------------------------------------------------------------------

  /**
   * Assert (using {@code junit.framework.Assert}} that all fields of
   * {@code message} are cleared, and that getting the fields returns their
   * default values.
   */
  public static void assertClear(TestAllTypes message) {
    // hasBlah() should initially be false for all optional fields.
    Assert.assertFalse(message.hasOptionalInt32   ());
    Assert.assertFalse(message.hasOptionalInt64   ());
    Assert.assertFalse(message.hasOptionalUint32  ());
    Assert.assertFalse(message.hasOptionalUint64  ());
    Assert.assertFalse(message.hasOptionalSint32  ());
    Assert.assertFalse(message.hasOptionalSint64  ());
    Assert.assertFalse(message.hasOptionalFixed32 ());
    Assert.assertFalse(message.hasOptionalFixed64 ());
    Assert.assertFalse(message.hasOptionalSfixed32());
    Assert.assertFalse(message.hasOptionalSfixed64());
    Assert.assertFalse(message.hasOptionalFloat   ());
    Assert.assertFalse(message.hasOptionalDouble  ());
    Assert.assertFalse(message.hasOptionalBool    ());
    Assert.assertFalse(message.hasOptionalString  ());
    Assert.assertFalse(message.hasOptionalBytes   ());

    Assert.assertFalse(message.hasOptionalGroup         ());
    Assert.assertFalse(message.hasOptionalNestedMessage ());
    Assert.assertFalse(message.hasOptionalForeignMessage());
    Assert.assertFalse(message.hasOptionalImportMessage ());

    Assert.assertFalse(message.hasOptionalNestedEnum ());
    Assert.assertFalse(message.hasOptionalForeignEnum());
    Assert.assertFalse(message.hasOptionalImportEnum ());

    Assert.assertFalse(message.hasOptionalStringPiece());
    Assert.assertFalse(message.hasOptionalCord());

    // Optional fields without defaults are set to zero or something like it.
    Assert.assertEquals(0    , message.getOptionalInt32   ());
    Assert.assertEquals(0    , message.getOptionalInt64   ());
    Assert.assertEquals(0    , message.getOptionalUint32  ());
    Assert.assertEquals(0    , message.getOptionalUint64  ());
    Assert.assertEquals(0    , message.getOptionalSint32  ());
    Assert.assertEquals(0    , message.getOptionalSint64  ());
    Assert.assertEquals(0    , message.getOptionalFixed32 ());
    Assert.assertEquals(0    , message.getOptionalFixed64 ());
    Assert.assertEquals(0    , message.getOptionalSfixed32());
    Assert.assertEquals(0    , message.getOptionalSfixed64());
    Assert.assertEquals(0    , message.getOptionalFloat   (), 0.0);
    Assert.assertEquals(0    , message.getOptionalDouble  (), 0.0);
    Assert.assertEquals(false, message.getOptionalBool    ());
    Assert.assertEquals(null   , message.getOptionalString  ());
    Assert.assertEquals(null, message.getOptionalBytes());
    Assert.assertEquals(null, message.getOptionalNestedEnum ());
    Assert.assertEquals(null, message.getOptionalForeignEnum());
    Assert.assertEquals(null, message.getOptionalImportEnum());
    Assert.assertEquals(null, message.getOptionalStringPiece());
    Assert.assertEquals(null, message.getOptionalCord());

    // Embedded messages should also be clear.
    Assert.assertFalse(message.getOptionalGroup         ().hasA());
    Assert.assertFalse(message.getOptionalNestedMessage ().hasBb());
    Assert.assertFalse(message.getOptionalForeignMessage().hasC());
    Assert.assertFalse(message.getOptionalImportMessage ().hasD());

    Assert.assertEquals(0, message.getOptionalGroup         ().getA());
    Assert.assertEquals(0, message.getOptionalNestedMessage ().getBb());
    Assert.assertEquals(0, message.getOptionalForeignMessage().getC());
    Assert.assertEquals(0, message.getOptionalImportMessage ().getD());



    // Repeated fields are empty.
    Assert.assertEquals(0, message.getRepeatedInt32List().size   ());
    Assert.assertEquals(0, message.getRepeatedInt64List().size   ());
    Assert.assertEquals(0, message.getRepeatedUint32List().size  ());
    Assert.assertEquals(0, message.getRepeatedUint64List().size  ());
    Assert.assertEquals(0, message.getRepeatedSint32List().size  ());
    Assert.assertEquals(0, message.getRepeatedSint64List().size  ());
    Assert.assertEquals(0, message.getRepeatedFixed32List().size ());
    Assert.assertEquals(0, message.getRepeatedFixed64List().size ());
    Assert.assertEquals(0, message.getRepeatedSfixed32List().size());
    Assert.assertEquals(0, message.getRepeatedSfixed64List().size());
    Assert.assertEquals(0, message.getRepeatedFloatList().size   ());
    Assert.assertEquals(0, message.getRepeatedDoubleList().size  ());
    Assert.assertEquals(0, message.getRepeatedBoolList().size    ());
    Assert.assertEquals(0, message.getRepeatedStringList().size  ());
    Assert.assertEquals(0, message.getRepeatedBytesList().size   ());

    Assert.assertEquals(0, message.getRepeatedGroupList().size         ());
    Assert.assertEquals(0, message.getRepeatedNestedMessageList().size ());
    Assert.assertEquals(0, message.getRepeatedForeignMessageList().size());
    Assert.assertEquals(0, message.getRepeatedImportMessageList().size ());
    Assert.assertEquals(0, message.getRepeatedNestedEnumList().size    ());
    Assert.assertEquals(0, message.getRepeatedForeignEnumList().size   ());
    Assert.assertEquals(0, message.getRepeatedImportEnumList().size    ());

    Assert.assertEquals(0, message.getRepeatedStringPieceList().size());
    Assert.assertEquals(0, message.getRepeatedCordList().size());

    // hasBlah() should also be false for all default fields.
    Assert.assertFalse(message.hasDefaultInt32   ());
    Assert.assertFalse(message.hasDefaultInt64   ());
    Assert.assertFalse(message.hasDefaultUint32  ());
    Assert.assertFalse(message.hasDefaultUint64  ());
    Assert.assertFalse(message.hasDefaultSint32  ());
    Assert.assertFalse(message.hasDefaultSint64  ());
    Assert.assertFalse(message.hasDefaultFixed32 ());
    Assert.assertFalse(message.hasDefaultFixed64 ());
    Assert.assertFalse(message.hasDefaultSfixed32());
    Assert.assertFalse(message.hasDefaultSfixed64());
    Assert.assertFalse(message.hasDefaultFloat   ());
    Assert.assertFalse(message.hasDefaultDouble  ());
    Assert.assertFalse(message.hasDefaultBool    ());
    Assert.assertFalse(message.hasDefaultString  ());
    Assert.assertFalse(message.hasDefaultBytes   ());

    Assert.assertFalse(message.hasDefaultNestedEnum ());
    Assert.assertFalse(message.hasDefaultForeignEnum());
    Assert.assertFalse(message.hasDefaultImportEnum ());

    Assert.assertFalse(message.hasDefaultStringPiece());
    Assert.assertFalse(message.hasDefaultCord());

    // Fields with defaults have their default values (duh).
    Assert.assertEquals( 41    , message.getDefaultInt32   ());
    Assert.assertEquals( 42    , message.getDefaultInt64   ());
    Assert.assertEquals( 43    , message.getDefaultUint32  ());
    Assert.assertEquals( 44    , message.getDefaultUint64  ());
    Assert.assertEquals(-45    , message.getDefaultSint32  ());
    Assert.assertEquals( 46    , message.getDefaultSint64  ());
    Assert.assertEquals( 47    , message.getDefaultFixed32 ());
    Assert.assertEquals( 48    , message.getDefaultFixed64 ());
    Assert.assertEquals( 49    , message.getDefaultSfixed32());
    Assert.assertEquals(-50    , message.getDefaultSfixed64());
    Assert.assertEquals( 51.5  , message.getDefaultFloat   (), 0.0);
    Assert.assertEquals( 52e3  , message.getDefaultDouble  (), 0.0);
    Assert.assertEquals(true   , message.getDefaultBool    ());
    Assert.assertEquals("hello", message.getDefaultString  ());
    Assert.assertEquals(toBytes("world"), message.getDefaultBytes());

    Assert.assertEquals(TestAllTypes.NestedEnum.BAR, message.getDefaultNestedEnum ());
    Assert.assertEquals(ForeignEnum.FOREIGN_BAR, message.getDefaultForeignEnum());
    Assert.assertEquals(ImportEnum.IMPORT_BAR, message.getDefaultImportEnum());

    Assert.assertEquals("abc", message.getDefaultStringPiece());
    Assert.assertEquals("123", message.getDefaultCord());
  }

  // -------------------------------------------------------------------

  /**
   * Assert (using {@code junit.framework.Assert}} that all fields of
   * {@code message} are set to the values assigned by {@code setAllFields}
   * followed by {@code modifyRepeatedFields}.
   */
  public static void assertRepeatedFieldsModified(TestAllTypes message) {
    // ModifyRepeatedFields only sets the second repeated element of each
    // field.  In addition to verifying this, we also verify that the first
    // element and size were *not* modified.
    Assert.assertEquals(2, message.getRepeatedInt32List().size   ());
    Assert.assertEquals(2, message.getRepeatedInt64List().size   ());
    Assert.assertEquals(2, message.getRepeatedUint32List().size  ());
    Assert.assertEquals(2, message.getRepeatedUint64List().size  ());
    Assert.assertEquals(2, message.getRepeatedSint32List().size  ());
    Assert.assertEquals(2, message.getRepeatedSint64List().size  ());
    Assert.assertEquals(2, message.getRepeatedFixed32List().size ());
    Assert.assertEquals(2, message.getRepeatedFixed64List().size ());
    Assert.assertEquals(2, message.getRepeatedSfixed32List().size());
    Assert.assertEquals(2, message.getRepeatedSfixed64List().size());
    Assert.assertEquals(2, message.getRepeatedFloatList().size   ());
    Assert.assertEquals(2, message.getRepeatedDoubleList().size  ());
    Assert.assertEquals(2, message.getRepeatedBoolList().size    ());
    Assert.assertEquals(2, message.getRepeatedStringList().size  ());
    Assert.assertEquals(2, message.getRepeatedBytesList().size   ());

    Assert.assertEquals(2, message.getRepeatedGroupList().size         ());
    Assert.assertEquals(2, message.getRepeatedNestedMessageList().size ());
    Assert.assertEquals(2, message.getRepeatedForeignMessageList().size());
    Assert.assertEquals(2, message.getRepeatedImportMessageList().size ());
    Assert.assertEquals(2, message.getRepeatedNestedEnumList().size    ());
    Assert.assertEquals(2, message.getRepeatedForeignEnumList().size   ());
    Assert.assertEquals(2, message.getRepeatedImportEnumList().size    ());

    Assert.assertEquals(2, message.getRepeatedStringPieceList().size());
    Assert.assertEquals(2, message.getRepeatedCordList().size());

    Assert.assertEquals(201  , (int)message.getRepeatedInt32List().get   (0));
    Assert.assertEquals(202L , (long)message.getRepeatedInt64List().get   (0));
    Assert.assertEquals(203  , (int)message.getRepeatedUint32List().get  (0));
    Assert.assertEquals(204L , (long)message.getRepeatedUint64List().get  (0));
    Assert.assertEquals(205  , (int)message.getRepeatedSint32List().get  (0));
    Assert.assertEquals(206L , (long)message.getRepeatedSint64List().get  (0));
    Assert.assertEquals(207  , (int)message.getRepeatedFixed32List().get (0));
    Assert.assertEquals(208L , (long)message.getRepeatedFixed64List().get (0));
    Assert.assertEquals(209  , (int)message.getRepeatedSfixed32List().get(0));
    Assert.assertEquals(210L , (long)message.getRepeatedSfixed64List().get(0));
    Assert.assertEquals(211F , message.getRepeatedFloatList().get   (0));
    Assert.assertEquals(212D , message.getRepeatedDoubleList().get  (0));
    Assert.assertEquals(true , (boolean)message.getRepeatedBoolList().get    (0));
    Assert.assertEquals("215", message.getRepeatedStringList().get  (0));
    Assert.assertEquals(toBytes("216"), message.getRepeatedBytesList().get(0));

    Assert.assertEquals(217, message.getRepeatedGroupList().get         (0).getA());
    Assert.assertEquals(218, message.getRepeatedNestedMessageList().get (0).getBb());
    Assert.assertEquals(219, message.getRepeatedForeignMessageList().get(0).getC());
    Assert.assertEquals(220, message.getRepeatedImportMessageList().get (0).getD());

    Assert.assertEquals(TestAllTypes.NestedEnum.BAR, message.getRepeatedNestedEnumList().get (0));
    Assert.assertEquals(ForeignEnum.FOREIGN_BAR, message.getRepeatedForeignEnumList().get(0));
    Assert.assertEquals(ImportEnum.IMPORT_BAR, message.getRepeatedImportEnumList().get(0));

    Assert.assertEquals("224", message.getRepeatedStringPieceList().get(0));
    Assert.assertEquals("225", message.getRepeatedCordList().get(0));

    // Actually verify the second (modified) elements now.
    Assert.assertEquals(501  , (int)message.getRepeatedInt32List().get   (1));
    Assert.assertEquals(502L , (long)message.getRepeatedInt64List().get   (1));
    Assert.assertEquals(503  , (int)message.getRepeatedUint32List().get  (1));
    Assert.assertEquals(504L , (long)message.getRepeatedUint64List().get  (1));
    Assert.assertEquals(505  , (int)message.getRepeatedSint32List().get  (1));
    Assert.assertEquals(506L , (long)message.getRepeatedSint64List().get  (1));
    Assert.assertEquals(507  , (int)message.getRepeatedFixed32List().get (1));
    Assert.assertEquals(508L , (long)message.getRepeatedFixed64List().get (1));
    Assert.assertEquals(509  , (int)message.getRepeatedSfixed32List().get(1));
    Assert.assertEquals(510L , (long)message.getRepeatedSfixed64List().get(1));
    Assert.assertEquals(511F , message.getRepeatedFloatList().get   (1));
    Assert.assertEquals(512D , message.getRepeatedDoubleList().get  (1));
    Assert.assertEquals(true , (boolean)message.getRepeatedBoolList().get    (1));
    Assert.assertEquals("515", message.getRepeatedStringList().get  (1));
    Assert.assertEquals(toBytes("516"), message.getRepeatedBytesList().get(1));

    Assert.assertEquals(517, message.getRepeatedGroupList().get         (1).getA());
    Assert.assertEquals(518, message.getRepeatedNestedMessageList().get (1).getBb());
    Assert.assertEquals(519, message.getRepeatedForeignMessageList().get(1).getC());
    Assert.assertEquals(520, message.getRepeatedImportMessageList().get (1).getD());

    Assert.assertEquals(TestAllTypes.NestedEnum.FOO, message.getRepeatedNestedEnumList().get (1));
    Assert.assertEquals(ForeignEnum.FOREIGN_FOO, message.getRepeatedForeignEnumList().get(1));
    Assert.assertEquals(ImportEnum.IMPORT_FOO, message.getRepeatedImportEnumList().get(1));

    Assert.assertEquals("524", message.getRepeatedStringPieceList().get(1));
    Assert.assertEquals("525", message.getRepeatedCordList().get(1));
  }

  // ===================================================================
  // Like above, but for extensions

  // Java gets confused with things like assertEquals(int, Integer):  it can't
  // decide whether to call assertEquals(int, int) or assertEquals(Object,
  // Object).  So we define these methods to help it.
  private static void assertEqualsExactType(int a, int b) {
    Assert.assertEquals(a, b);
  }
  private static void assertEqualsExactType(long a, long b) {
    Assert.assertEquals(a, b);
  }
  private static void assertEqualsExactType(float a, float b) {
    Assert.assertEquals(a, b, 0.0);
  }
  private static void assertEqualsExactType(double a, double b) {
    Assert.assertEquals(a, b, 0.0);
  }
  private static void assertEqualsExactType(boolean a, boolean b) {
    Assert.assertEquals(a, b);
  }
  private static void assertEqualsExactType(String a, String b) {
    Assert.assertEquals(a, b);
  }
  private static void assertEqualsExactType(Buffer a, Buffer b) {
    Assert.assertEquals(a, b);
  }
  private static void assertEqualsExactType(TestAllTypes.NestedEnum a,
                                            TestAllTypes.NestedEnum b) {
    Assert.assertEquals(a, b);
  }
  private static void assertEqualsExactType(ForeignEnum a, ForeignEnum b) {
    Assert.assertEquals(a, b);
  }
  private static void assertEqualsExactType(ImportEnum a, ImportEnum b) {
    Assert.assertEquals(a, b);
  }

  /**
   * @param filePath The path relative to
   * {@link com.google.testing.util.TestUtil#getDefaultSrcDir}.
   */
  public static String readTextFromFile(String filePath) {
    return readBytesFromFile(filePath).toStringUtf8();
  }

  private static File getTestDataDir() {
    // Search each parent directory looking for "src/google/protobuf".
    File ancestor = new File(".");
    try {
      ancestor = ancestor.getCanonicalFile();
    } catch (IOException e) {
      throw new RuntimeException(
        "Couldn't get canonical name of working directory.", e);
    }
    while (ancestor != null && ancestor.exists()) {
      if (new File(ancestor, "src/google/protobuf").exists()) {
        return new File(ancestor, "src/google/protobuf/testdata");
      }
      ancestor = ancestor.getParentFile();
    }

    throw new RuntimeException(
      "Could not find golden files.  This test must be run from within the " +
      "protobuf source package so that it can read test data files from the " +
      "C++ source tree.");
  }

  /**
   * @param filePath The path relative to
   * {@link com.google.testing.util.TestUtil#getDefaultSrcDir}.
   */
  public static Buffer readBytesFromFile(String filename) {
    File fullPath = new File(getTestDataDir(), filename);
    try {
      RandomAccessFile file = new RandomAccessFile(fullPath, "r");
      byte[] content = new byte[(int) file.length()];
      file.readFully(content);
      return new Buffer(content);
    } catch (IOException e) {
      // Throw a RuntimeException here so that we can call this function from
      // static initializers.
      throw new IllegalArgumentException(
        "Couldn't read file: " + fullPath.getPath(), e);
    }
  }

  /**
   * Get the bytes of the "golden message".  This is a serialized TestAllTypes
   * with all fields set as they would be by
   * {@link setAllFields(TestAllTypes.Builder)}, but it is loaded from a file
   * on disk rather than generated dynamically.  The file is actually generated
   * by C++ code, so testing against it verifies compatibility with C++.
   */
  public static Buffer getGoldenMessage() {
    if (goldenMessage == null) {
      goldenMessage = readBytesFromFile("golden_message");
    }
    return goldenMessage;
  }
  private static Buffer goldenMessage = null;
}
