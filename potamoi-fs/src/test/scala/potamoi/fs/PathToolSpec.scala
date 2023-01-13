package potamoi.fs

import org.scalatest.wordspec.AnyWordSpec
import potamoi.fs.refactor.paths.*
import potamoi.PotaErr

class PathToolSpec extends AnyWordSpec:

  "PathTool" should {

    "purePath" in {
      val purePath = rmSchema andThen rmFirstSlash
      assert(purePath("s3://bucket/xx/xx.jar") == "bucket/xx/xx.jar")
      assert(purePath("s3:///bucket/xx/xx.jar") == "bucket/xx/xx.jar")
      assert(purePath("/bucket/xx/xx.jar") == "bucket/xx/xx.jar")
    }

    "rmFirstSlash" in {
      assert(rmFirstSlash("/xx/xx.jar") == "xx/xx.jar")
      assert(rmFirstSlash("xx/xx.jar") == "xx/xx.jar")
      assert(rmFirstSlash("") == "")
    }

  }