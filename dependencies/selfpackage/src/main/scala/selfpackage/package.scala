import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner
import java.io._
import java.util.jar._
import java.util.zip._
import scala.collection.JavaConverters._
import scala.util._
import java.nio.file._
import java.nio.file.attribute._

package object selfpackage {

  def mainClass: Option[String] =
    Thread.getAllStackTraces.asScala.find(_._1.getName == "main").map {
      case (threadName, stackTrace) =>
        stackTrace.last.getClassName
    }

  def commonprefix(s1: String, s2: String) =
    s1 zip s2 takeWhile (x => x._1 == x._2)

  def writeJar(input: File, out: File, removePrefix: String): File = {
    val tmp = File.createTempFile("self", "")

    val fos = new BufferedOutputStream(new FileOutputStream(tmp))
    val jos = new JarOutputStream(fos);
    val fileVisitor = new SimpleFileVisitor[Path] {
      override def visitFile(path: Path, attrs: BasicFileAttributes) = {
        val file = path.toFile
        val br = new BufferedInputStream(new FileInputStream(file))
        // Find the relative path on the class path
        val fileNameInJar = {
          val fa = file.getAbsolutePath
          fa.drop(removePrefix.size)
        }
        jos.putNextEntry(new JarEntry(fileNameInJar));
        var c = br.read
        while (c != -1) {
          jos.write(c);
          c = br.read
        }
        br.close
        jos.closeEntry
        FileVisitResult.CONTINUE
      }
    }
    Files.walkFileTree(input.toPath, fileVisitor)
    jos.close
    fos.close
    Files.copy(tmp.toPath, out.toPath)
    tmp.delete
    out
  }

  def write(out: File): Unit = {
    val mainClassName = mainClass.get
    val classpathFolders = ClassLoader.getSystemClassLoader
      .asInstanceOf[java.net.URLClassLoader]
      .getURLs
      .map(_.toString.stripPrefix("file:"))
      .toList

    val classpathFilesOrFolders =
      (new FastClasspathScanner()).getUniqueClasspathElements.asScala

    val tmp = File.createTempFile("self", "")

    val files = classpathFilesOrFolders.filter(_.isFile)
    val folders = classpathFilesOrFolders.filter(_.isDirectory)

    val jarFromFolders = folders.zipWithIndex.map {
      case (folder, idx) =>
        val root =
          classpathFolders.find(x => folder.getAbsolutePath.startsWith(x)).get
        writeJar(folder,
                 new File(tmp.getAbsolutePath + "." + idx + ".jar"),
                 root)
    }

    val selfExtraction = """|#!/usr/bin/env bash
    |mkdir -p $0-extract
    |unzip -o  $0 -d $0-extract 2> /dev/null 1> /dev/null
    |chmod u+x $0-extract/entrypoint
    |exec $0-extract/entrypoint $@
    """.stripMargin

    val fos = new BufferedOutputStream(new FileOutputStream(tmp))
    fos.write(selfExtraction.getBytes("UTF-8"))
    val zos = new ZipOutputStream(fos);
    val libs = (jarFromFolders ++ files).map { file =>
      val br = new BufferedInputStream(new FileInputStream(file))

      zos.putNextEntry(new ZipEntry("lib/" + file.getName));
      var c = br.read
      while (c != -1) {
        zos.write(c);
        c = br.read
      }
      br.close
      zos.closeEntry
      "lib/" + file.getName
    }

    val scriptFunctions = """

    declare -a residual_args
    declare -a java_args
    addJava () {
      java_args+=( "$1" )
    }
    addResidual () {
      residual_args+=( "$1" )
    }
    process_args () {
      local no_more_snp_opts=0
      while [[ $# -gt 0 ]]; do
        case "$1" in
                 --) shift && no_more_snp_opts=1 && break ;;
                 -D*) addJava "$1" && shift ;;
                -J*) addJava "${1:2}" && shift ;;
                  *) addResidual "$1" && shift ;;
        esac
      done

      if [[ no_more_snp_opts ]]; then
        while [[ $# -gt 0 ]]; do
          addResidual "$1" && shift
        done
      fi
    }
    if [[ "$JAVA_OPTS" != "" ]]; then
      java_opts="${JAVA_OPTS}"
    fi

    process_args "$@"
    set -- "${residual_args[@]}"

    """

    val script = s"""|#!/usr/bin/env bash
    |$scriptFunctions
    |script_root=$$(dirname $$0)
    |classpath=${libs.map(l => "${script_root}/" + l).mkString(":")}
    |exec java $${java_opts[@]} "$${java_args[@]}" -cp $$classpath $mainClassName "$${residual_args[@]}"
    """.stripMargin

    zos.putNextEntry(new ZipEntry("entrypoint"))
    zos.write(script.getBytes("UTF-8"))
    zos.closeEntry
    zos.close
    fos.close
    Files.copy(tmp.toPath, out.toPath, StandardCopyOption.REPLACE_EXISTING)

  }
}
