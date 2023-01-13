package potamoi.common

import org.scalatest.wordspec.AnyWordSpec
import potamoi.common.NumExtension.*
import zio.test.{suite, ZIOSpecDefault}
import zio.test.Assertion.*

class NumExtensionSpec extends AnyWordSpec:

  "ensureMin for int" in {
    assert(10.ensureIntMin(5) == 10)
    assert(10.ensureIntMin(10) == 10)
    assert(10.ensureIntMin(15) == 15)
  }

  "ensureMoreThan for int" in {
    assert(10.ensureIntOr(_ > 5, 0) == 10)
    assert(10.ensureIntOr(_ > 10, 0) == 0)
    assert(10.ensureIntOr(_ > 15, 0) == 0)
  }

  "ensureMin for double" in {
    assert(10.0.ensureDoubleMin(5.0) == 10.0)
    assert(10.0.ensureDoubleMin(10.0) == 10.0)
    assert(10.0.ensureDoubleMin(15.0) == 15.0)
  }

  "ensureMoreThan for double" in {
    assert(10.0.ensureDoubleOr(_ > 5.0, 0.0) == 10.0)
    assert(10.0.ensureDoubleOr(_ > 10.0, 0.0) == 0.0)
    assert(10.0.ensureDoubleOr(_ > 15.0, 0.0) == 0.0)
  }
