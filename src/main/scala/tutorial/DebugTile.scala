package tutorial

import java.io.{File, FileOutputStream}
import java.nio.ByteBuffer

import org.apache.hadoop.hbase.client.Scan
import org.openstreetmap.osmosis.core.util.TileCalculator
import xyz.TileCalculator.Tile

/**
  * Created by willtemperley@gmail.com on 21-Nov-16.
  */
object DebugTile {

  def main(args: Array[String]): Unit = {
    val tab = AccessHbase.getTable("buffer14")

    val scan = new Scan()
    scan.addFamily(Tile.cf)
    val res = tab.getScanner(scan)

    val iterator = res.iterator()
    while (iterator.hasNext) {

      val res = iterator.next()
      val v = res.getValue(Tile.cf, Tile.cimg)

      val t = new Tile(res.getRow)
      println(t)

      writeDebugTile(t, v)

    }


  }
  private def writeDebugTile(tile: Tile, bytes: Array[Byte]) {
    val f: File = new File("/tmp/ras/mr-" + tile.toString + ".png")
    val fileOutputStream: FileOutputStream = new FileOutputStream(f)
    for (aByte <- bytes) {
      fileOutputStream.write(aByte)
    }
  }

}
