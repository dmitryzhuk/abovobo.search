package org.abovobo.search.impl

import java.io.File
import java.io.FileInputStream
import java.io.ByteArrayOutputStream
import java.util.zip.GZIPOutputStream
import java.util.zip.DeflaterOutputStream
import java.util.zip.Deflater

object CompressorsTest extends App {
  val file = new File("/Users/leha/test1.txt")
  
  def gzip(array: Array[Byte]) = {
    val baos = new ByteArrayOutputStream();
    val zos = new DeflaterOutputStream(baos, new Deflater(9, true))
    
    zos.write(array)
    zos.finish();
    zos.flush();

    baos.toByteArray
  }
  
  def test(name: String, file: File, pack: Array[Byte] => Array[Byte], unpack: Array[Byte] => Array[Byte] = null) = {
    println("----- testing " + name)

    val buff = new Array[Byte](15000)
    val size = new FileInputStream(file).read(buff)
    println("src size: " + size)
    val compressed = pack(shrink(buff, size))
    println("compressed size: " + compressed.length)
    println("ratio: " + (compressed.length + 0.0) / size)
    
    if (unpack != null) {
      // verification
      if (shrink(buff, size).deep != compressed.deep) {
        throw new RuntimeException("wrong compression")
      } else {
        println("validated")
      }
    }
  }
  
  def shrink(array: Array[Byte], size: Int) = {
    val t = new Array[Byte](size)
    System.arraycopy(array, 0, t, 0, size)
    t
  }
  
  test("gzip", file, gzip)
}