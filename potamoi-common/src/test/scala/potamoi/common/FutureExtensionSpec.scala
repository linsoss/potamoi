package potamoi.common

import potamoi.common.FutureExtension.*
import zio.*

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class FutureExtensionSpec extends munit.FunSuite:

  test("asZIO") {
    val ef = Unsafe.unsafe { implicit u =>
      Runtime.default.unsafe.run(Future(1 + 1).asZIO).getOrThrow()
    }
    assert(ef == 2)
  }

  test("blocking result") {
    assert(Future(1 + 1).blocking() == 2)
  }

  test("safeBlocking result") {
    case object Boom extends Exception
    assert(Future(throw Boom).safeBlocking() == Left(Boom))
  }
