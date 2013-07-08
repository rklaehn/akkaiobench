package iobench

import akka.io.Tcp._
import java.io.File

class WriteFileAckBenchmarkActor extends BenchmarkActor {
  import BenchmarkActor._
  import MappedFileAckBenchmarkActor._

  def receive = {
    case SendFile(file, connection, blockSize) =>
      val size = new File(file).length.toInt
      val t0 = System.nanoTime()
      var offset = 0
      context watch connection
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
            println(s"Sending data from $min to $max")
            connection ! WriteFile(file, min, max-min, SendBlockAt(max))
          }
        case msg@CommandFailed(cmd) =>
          println(s"Got CommandFailed $cmd. Sending ResumeWriting to connection!")
          System.gc()
          connection ! ResumeWriting
          log.debug(msg.toString)
        case msg@WritingResumed =>
          println(s"Got WritingResumed. Re-sending block at $offset!")
          // trigger send of the next block
          self ! SendBlockAt(offset)
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

object WriteFileAckBenchmarkActor {
  case class Ack(offset:Int) extends Event
}
