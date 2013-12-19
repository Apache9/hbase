cc_library(
  name = "thrift_service",
  srcs = [
    "src/main/native/thrift/gen-cpp/Hbase.cpp",
    "src/main/native/thrift/gen-cpp/Hbase_constants.cpp",
    "src/main/native/thrift/gen-cpp/Hbase_types.cpp",
  ],
  deps = [
    "//thirdparty/thrift:thrift",
  ],
  extra_cppflags = [
    "-Wno-return-type",
  ],
)

cc_library(
  name = "thrift2_service",
  srcs = [
    "src/main/native/thrift2/gen-cpp/hbase_constants.cpp",
    "src/main/native/thrift2/gen-cpp/hbase_types.cpp",
    "src/main/native/thrift2/gen-cpp/THBaseService.cpp",
  ],
  deps = [
    "//thirdparty/thrift:thrift",
  ],
  extra_cppflags = [
    "-Wno-return-type",
  ],
)
