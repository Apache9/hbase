# JUnit4 to JUnit5 Migration Task List

This file tracks the migration of JUnit4 tests to JUnit5 in hbase-common module.

## Migration Notes

1. **Remove HBaseClassTestRule**: All `@ClassRule` annotations referencing `HBaseClassTestRule` should be removed.

2. **Assert Parameter Order**: In JUnit4, assert methods have the message parameter first, while in JUnit5 it's last. For example:
   - JUnit4: `assertEquals("message", expected, actual)`
   - JUnit5: `assertEquals(expected, actual, "message")`

3. **Import Changes**: Replace JUnit4 imports with JUnit5 equivalents:
   - `import org.junit.Test` → `import org.junit.jupiter.api.Test`
   - `import org.junit.Before` → `import org.junit.jupiter.api.BeforeEach`
   - `import org.junit.After` → `import org.junit.jupiter.api.AfterEach`
   - `import org.junit.BeforeClass` → `import org.junit.jupiter.api.BeforeAll`
   - `import org.junit.AfterClass` → `import org.junit.jupiter.api.AfterAll`

4. **Test Method Visibility**: JUnit5 test methods can be package-private (no visibility modifier needed).

5. **Expected Exceptions**: Replace `@Test(expected = Exception.class)` with `assertThrows()`.

6. **Replace Category With Tag**: Replace JUnit4 `@Category({XXXTests.class, YYYTests.class})` with JUnit5 `@Tag(XXXTests.TAG)` and `@Tag(YYYTests.TAG)`.

7. **Tag Symbol Conflicts**: Remove the import for `org.junit.jupiter.api.Tag`, at the beginning of class, use `@org.junit.jupiter.api.Tag(XXXTests.TAG)` instead.

8. **Parameterized Tests Migration**: Replace JUnit4 parameterized tests with JUnit5 approach:
   - Replace JUnit4 `@RunWith(Parameterized.class)` with JUnit5 @ParameterizedClass
   - Replace JUnit4 `@Parameter` with JUnit5 `@Parameter`
   - Remove JUnit4 `@Parameters`, add a JUnit5 `@MethodSource` annotation on the class to reference the method
   - Change the return value of the data source method to Stream<Arguments> and fix its implementation

## Files to Migrate (46 total)

### Core Tests
- [x] `src/test/java/org/apache/hadoop/hbase/TestHDFSBlocksDistribution.java`
- [x] `src/test/java/org/apache/hadoop/hbase/TestHBaseConfiguration.java`
- [x] `src/test/java/org/apache/hadoop/hbase/TestHBaseClassTestRule.java` (SKIPPED as requested)
- [x] `src/test/java/org/apache/hadoop/hbase/TestCompoundConfiguration.java`
- [x] `src/test/java/org/apache/hadoop/hbase/TestClassFinder.java`
- [x] `src/test/java/org/apache/hadoop/hbase/TestChoreService.java`
- [x] `src/test/java/org/apache/hadoop/hbase/TestCellComparator.java`
- [x] `src/test/java/org/apache/hadoop/hbase/TestByteBufferKeyValue.java`

### Type System Tests
- [x] `src/test/java/org/apache/hadoop/hbase/types/TestStruct.java`
- [x] `src/test/java/org/apache/hadoop/hbase/types/TestRawBytes.java`
- [x] `src/test/java/org/apache/hadoop/hbase/types/TestOrderedNumeric.java`
- [x] `src/test/java/org/apache/hadoop/hbase/types/TestOrderedInt8.java`
- [x] `src/test/java/org/apache/hadoop/hbase/types/TestOrderedInt64.java`
- [x] `src/test/java/org/apache/hadoop/hbase/types/TestOrderedInt32.java`
- [x] `src/test/java/org/apache/hadoop/hbase/types/TestOrderedInt16.java`
- [x] `src/test/java/org/apache/hadoop/hbase/types/TestOrderedFloat64.java`
- [x] `src/test/java/org/apache/hadoop/hbase/types/TestOrderedFloat32.java`

### Utility Tests
- [x] `src/test/java/org/apache/hadoop/hbase/util/TestThreads.java`
- [x] `src/test/java/org/apache/hadoop/hbase/util/TestMovingAverage.java`
- [x] `src/test/java/org/apache/hadoop/hbase/util/TestClasses.java`
- [x] `src/test/java/org/apache/hadoop/hbase/util/AbstractHBaseToolTest.java`

### NIO Tests
- [x] `src/test/java/org/apache/hadoop/hbase/nio/TestSingleByteBuff.java`
- [x] `src/test/java/org/apache/hadoop/hbase/nio/TestMultiByteBuff.java`

### Network Tests
- [x] `src/test/java/org/apache/hadoop/hbase/net/TestAddress.java`

### Logging Tests
- [x] `src/test/java/org/apache/hadoop/hbase/logging/TestLog4jUtils.java`
- [x] `src/test/java/org/apache/hadoop/hbase/logging/TestJul2Slf4j.java`

### Configuration Tests
- [x] `src/test/java/org/apache/hadoop/hbase/conf/TestConfigurationManager.java`
- [x] `src/test/java/org/apache/hadoop/hbase/conf/TestConfigKey.java`

### Codec Tests
- [x] `src/test/java/org/apache/hadoop/hbase/codec/TestKeyValueCodecWithTags.java`
- [x] `src/test/java/org/apache/hadoop/hbase/codec/TestKeyValueCodec.java`
- [x] `src/test/java/org/apache/hadoop/hbase/codec/TestCellCodecWithTags.java`
- [x] `src/test/java/org/apache/hadoop/hbase/codec/TestCellCodec.java`

### IO Tests
- [x] `src/test/java/org/apache/hadoop/hbase/io/util/TestLRUDictionary.java`
- [x] `src/test/java/org/apache/hadoop/hbase/io/encoding/TestEncodedDataBlock.java`

### TLS/Crypto Tests
- [x] `src/test/java/org/apache/hadoop/hbase/io/crypto/tls/TestX509Util.java`
- [x] `src/test/java/org/apache/hadoop/hbase/io/crypto/tls/TestPKCS12FileLoader.java`
- [x] `src/test/java/org/apache/hadoop/hbase/io/crypto/tls/TestPEMFileLoader.java`
- [x] `src/test/java/org/apache/hadoop/hbase/io/crypto/tls/TestKeyStoreFileType.java`
- [x] `src/test/java/org/apache/hadoop/hbase/io/crypto/tls/TestJKSFileLoader.java`
- [ ] `src/test/java/org/apache/hadoop/hbase/io/crypto/tls/TestHBaseTrustManager.java`
- [ ] `src/test/java/org/apache/hadoop/hbase/io/crypto/tls/TestHBaseHostnameVerifier.java`
- [ ] `src/test/java/org/apache/hadoop/hbase/io/crypto/tls/TestFileKeyStoreLoaderBuilderProvider.java`
- [ ] `src/test/java/org/apache/hadoop/hbase/io/crypto/tls/TestBCFKSFileLoader.java`
- [ ] `src/test/java/org/apache/hadoop/hbase/io/crypto/aes/TestCommonsAES.java`
- [ ] `src/test/java/org/apache/hadoop/hbase/io/crypto/aes/TestAES.java`

### ZooKeeper Tests
- [ ] `src/test/java/org/apache/hadoop/hbase/zookeeper/TestZKConfig.java`
