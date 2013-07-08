package iobench

import java.io.RandomAccessFile
import java.nio.channels.FileChannel
import akka.io.Tcp._
import akka.util.ByteString
import akka.io.Tcp.CommandFailed

class MappedFileAckBenchmarkActor extends BenchmarkActor {
  import BenchmarkActor._
  import MappedFileAckBenchmarkActor._

  def receive = {
    case SendFile(file, connection, blockSize) =>
      val channel = new RandomAccessFile(file, "r").getChannel()
      val size = channel.size().toInt
      val buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, size)
      channel.close()
      val t0 = System.nanoTime()
      var offset = 0
      self ! SendBlockAt(0)
      context.become {
        case msg@SendBlockAt(x) =>
          if(x==size) {
            printResult(size, System.nanoTime() - t0)
            connection ! Close
          } else {
            offset = x
            val min = offset
            val max = (offset + blockSize) min size
            val slice = buffer.slice()
            slice.position(min)
            slice.limit(max)
            connection ! Write(ByteString.fromByteBuffer(slice), SendBlockAt(max))
          }
        case msg@CommandFailed(cmd) =>
          log.debug(msg.toString)
        case msg@PeerClosed =>
          context.stop(self)
          log.debug(msg.toString)
        case msg@Closed =>
          log.debug(msg.toString)
          context.stop(self)
        case msg =>
          log.debug(msg.toString)
      }
  }
}

object MappedFileAckBenchmarkActor {
  case class SendBlockAt(offset:Int) extends Event
}
