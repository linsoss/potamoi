package potamoi.common

import potamoi.common.PathTool.*

class PathToolSpec extends munit.FunSuite:

  test("purePath") {
    assert(purePath("s3://bucket/xx/xx.jar") == "bucket/xx/xx.jar")
    assert(purePath("s3:///bucket/xx/xx.jar") == "bucket/xx/xx.jar")
    assert(purePath("/bucket/xx/xx.jar") == "bucket/xx/xx.jar")
  }

  test("rmSlashPrefix") {
    assert(rmSlashPrefix("/xx/xx.jar") == "xx/xx.jar")
    assert(rmSlashPrefix("xx/xx.jar") == "xx/xx.jar")
    assert(rmSlashPrefix("") == "")
  }

  test("isS3Path") {
    assert(isS3Path("s3://bucket/xx/xx.jar"))
    assert(isS3Path("s3:///bucket/xx/xx.jar"))
    assert(isS3Path("s3a://bucket/xx/xx.jar"))
    assert(isS3Path("s3n://bucket/xx/xx.jar"))
    assert(isS3Path("s3p://bucket/xx/xx.jar"))
    assert(!isS3Path("hdfs://xx/xx.jar"))
    assert(!isS3Path("/xx/xx.jar"))
    assert(!isS3Path("file:///xx/xx.jar"))
    assert(!isS3Path(""))
  }

  test("reviseToS3pSchema") {
    assert(reviseToS3pSchema("s3://b1/file") == "s3p://b1/file")
    assert(reviseToS3pSchema("s3p://b1/file") == "s3p://b1/file")
    assert(reviseToS3pSchema("s3a://b1/file") == "s3p://b1/file")
    assert(reviseToS3pSchema("file:///b1/file") == "file:///b1/file")
    assert(reviseToS3pSchema("/b1/file") == "/b1/file")
  }