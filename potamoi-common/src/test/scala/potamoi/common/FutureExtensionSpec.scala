package potamoi.common

import org.scalatest.wordspec.AnyWordSpec
import potamoi.common.FutureExtension.*
import zio.*

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class FutureExtensionSpec extends AnyWordSpec:

  "FutureExtension" should {

    "asZIO" in {
      val ef = Unsafe.unsafe { implicit u =>
        Runtime.default.unsafe.run(Future(1 + 1).asZIO).getOrThrow()
      }
      assert(ef == 2)
    }

    "blocking result" in {
      assert(Future(1 + 1).blocking() == 2)
    }

    "safeBlocking result" in {
      case object Boom extends Exception
      assert(Future(throw Boom).safeBlocking() == Left(Boom))
    }

  }
