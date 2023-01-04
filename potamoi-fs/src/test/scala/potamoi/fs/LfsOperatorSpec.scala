package potamoi.fs

import potamoi.fs.OsTool.{randomDir, randomFile, rmFile}
import potamoi.fs.refactor.lfs
import potamoi.zios.*
import zio.ZIO
import zio.test.ZIOSpecDefault

import java.io.{File, FileWriter}
import scala.io.Source
import scala.reflect.io.Directory
import scala.util.{Random, Try, Using}

class LfsOperatorSpec extends munit.FunSuite:

  test("rm file") {
    randomFile { file =>
      lfs.rm(file.getPath).run
      assert(!file.exists())
    }
  }

  test("rm directory") {
    randomDir(0) { dir =>
      lfs.rm(dir.getPath).run
      assert(!dir.exists())
    }
    randomDir(5) { dir =>
      lfs.rm(dir.getPath).run
      assert(!dir.exists())
    }
  }

  test("rm not exist file/directory") {
    lfs.rm("test-233.txt").run
    lfs.rm("test-23/233").run
  }

  test("write content to file") {
    val file = File(s"${System.currentTimeMillis}.txt")
    lfs.write(file.getPath, "hello world 你好").run
    assert(Using(Source.fromFile(file))(_.mkString).get == "hello world 你好")
    rmFile(file)
  }

object OsTool:

  private val rand = Random()

  def randomFile(f: File => Any): Unit = {
    val file = new File(genRandomFile())
    f(file)
    rmFile(file)
  }

  def randomDir(fileSize: Int)(f: File => Any): Unit = {
    val dir = new File(genRandomDirectory(fileSize))
    f(dir)
    rmDir(dir)
  }

  def genRandomFile(dir: String = ""): String = {
    val fileName = if (dir.isEmpty) s"test-${System.currentTimeMillis()}.txt" else s"dir/test-${System.currentTimeMillis()}.txt"
    Using(new FileWriter(fileName)) { io =>
      io.write((1 to 100).map(_ => rand.nextString(20)).mkString("\n"))
    }
    fileName
  }

  def genRandomDirectory(fileSize: Int) = {
    val dir = s"test-${System.currentTimeMillis()}"
    new File(dir).mkdir()
    (0 until fileSize).map(_ => genRandomFile(dir))
    dir
  }

  def rmFile(file: File) = Try(file.delete())

  def rmDir(file: File) = Try(Directory(file).deleteRecursively())
