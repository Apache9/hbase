# JUnit4 to JUnit5 Migration Task List

This file tracks the migration of JUnit4 tests to JUnit5 in hbase-common module.

## Files to Migrate (46 total)

### Core Tests
- [x] `src/test/java/org/apache/hadoop/hbase/TestHDFSBlocksDistribution.java`
- [x] `src/test/java/org/apache/hadoop/hbase/TestHBaseConfiguration.java`
- [x] `src/test/java/org/apache/hadoop/hbase/TestHBaseClassTestRule.java` (SKIPPED as requested)
- [x] `src/test/java/org/apache/hadoop/hbase/TestCompoundConfiguration.java`
- [x] `src/test/java/org/apache/hadoop/hbase/TestClassFinder.java`
- [x] `src/test/java/org/apache/hadoop/hbase/TestChoreService.java`
- [x] `src/test/java/org/apache/hadoop/hbase/TestCellComparator.java`
- [ ] `src/test/java/org/apache/hadoop/hbase/TestByteBufferKeyValue.java`

### Type System Tests
- [ ] `src/test/java/org/apache/hadoop/hbase/types/TestStruct.java`
- [ ] `src/test/java/org/apache/hadoop/hbase/types/TestRawBytes.java`
- [ ] `src/test/java/org/apache/hadoop/hbase/types/TestOrderedNumeric.java`
- [ ] `src/test/java/org/apache/hadoop/hbase/types/TestOrderedInt8.java`
- [ ] `src/test/java/org/apache/hadoop/hbase/types/TestOrderedInt64.java`
- [ ] `src/test/java/org/apache/hadoop/hbase/types/TestOrderedInt32.java`
- [ ] `src/test/java/org/apache/hadoop/hbase/types/TestOrderedInt16.java`
- [ ] `src/test/java/org/apache/hadoop/hbase/types/TestOrderedFloat64.java`
- [ ] `src/test/java/org/apache/hadoop/hbase/types/TestOrderedFloat32.java`

### Utility Tests
- [ ] `src/test/java/org/apache/hadoop/hbase/util/TestThreads.java`
- [ ] `src/test/java/org/apache/hadoop/hbase/util/TestMovingAverage.java`
- [ ] `src/test/java/org/apache/hadoop/hbase/util/TestClasses.java`
- [ ] `src/test/java/org/apache/hadoop/hbase/util/AbstractHBaseToolTest.java`

### NIO Tests
- [ ] `src/test/java/org/apache/hadoop/hbase/nio/TestSingleByteBuff.java`
- [ ] `src/test/java/org/apache/hadoop/hbase/nio/TestMultiByteBuff.java`

### Network Tests
- [ ] `src/test/java/org/apache/hadoop/hbase/net/TestAddress.java`

### Logging Tests
- [ ] `src/test/java/org/apache/hadoop/hbase/logging/TestLog4jUtils.java`
- [ ] `src/test/java/org/apache/hadoop/hbase/logging/TestJul2Slf4j.java`

### Configuration Tests
- [ ] `src/test/java/org/apache/hadoop/hbase/conf/TestConfigurationManager.java`
- [ ] `src/test/java/org/apache/hadoop/hbase/conf/TestConfigKey.java`

### Codec Tests
- [ ] `src/test/java/org/apache/hadoop/hbase/codec/TestKeyValueCodecWithTags.java`
- [ ] `src/test/java/org/apache/hadoop/hbase/codec/TestKeyValueCodec.java`
- [ ] `src/test/java/org/apache/hadoop/hbase/codec/TestCellCodecWithTags.java`
- [ ] `src/test/java/org/apache/hadoop/hbase/codec/TestCellCodec.java`

### IO Tests
- [ ] `src/test/java/org/apache/hadoop/hbase/io/util/TestLRUDictionary.java`
- [ ] `src/test/java/org/apache/hadoop/hbase/io/encoding/TestEncodedDataBlock.java`

### TLS/Crypto Tests
- [ ] `src/test/java/org/apache/hadoop/hbase/io/crypto/tls/TestX509Util.java`
- [ ] `src/test/java/org/apache/hadoop/hbase/io/crypto/tls/TestPKCS12FileLoader.java`
- [ ] `src/test/java/org/apache/hadoop/hbase/io/crypto/tls/TestPEMFileLoader.java`
- [ ] `src/test/java/org/apache/hadoop/hbase/io/crypto/tls/TestKeyStoreFileType.java`
- [ ] `src/test/java/org/apache/hadoop/hbase/io/crypto/tls/TestJKSFileLoader.java`
- [ ] `src/test/java/org/apache/hadoop/hbase/io/crypto/tls/TestHBaseTrustManager.java`
- [ ] `src/test/java/org/apache/hadoop/hbase/io/crypto/tls/TestHBaseHostnameVerifier.java`
- [ ] `src/test/java/org/apache/hadoop/hbase/io/crypto/tls/TestFileKeyStoreLoaderBuilderProvider.java`
- [ ] `src/test/java/org/apache/hadoop/hbase/io/crypto/tls/TestBCFKSFileLoader.java`
- [ ] `src/test/java/org/apache/hadoop/hbase/io/crypto/aes/TestCommonsAES.java`
- [ ] `src/test/java/org/apache/hadoop/hbase/io/crypto/aes/TestAES.java`

### ZooKeeper Tests
- [ ] `src/test/java/org/apache/hadoop/hbase/zookeeper/TestZKConfig.java`

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

## Progress
**6/46 files migrated (13.0%)**
