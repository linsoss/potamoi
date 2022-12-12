package potamoi.common

import zio.test.{suite, ZIOSpecDefault}
import zio.test.Assertion.*
import potamoi.common.NumExtension.*

class NumExtensionSpec extends munit.FunSuite:
  
  test("ensureMin for int") {
    assert(10.ensureIntMin(5) == 10)
    assert(10.ensureIntMin(10) == 10)
    assert(10.ensureIntMin(15) == 15)
  }

  test("ensureMoreThan for int") {
    assert(10.ensureIntOr(_ > 5, 0) == 10)
    assert(10.ensureIntOr(_ > 10, 0) == 0)
    assert(10.ensureIntOr(_ > 15, 0) == 0)
  }

  test("ensureMin for double") {
    assert(10.0.ensureDoubleMin(5.0) == 10.0)
    assert(10.0.ensureDoubleMin(10.0) == 10.0)
    assert(10.0.ensureDoubleMin(15.0) == 15.0)
  }

  test("ensureMoreThan for double") {
    assert(10.0.ensureDoubleOr(_ > 5.0, 0.0) == 10.0)
    assert(10.0.ensureDoubleOr(_ > 10.0, 0.0) == 0.0)
    assert(10.0.ensureDoubleOr(_ > 15.0, 0.0) == 0.0)
  }
