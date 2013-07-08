package iobench

import akka.io.Tcp._
import java.io._

class WriteFileBenchmarkActor extends BenchmarkActor {
  import BenchmarkActor._
  import WriteFileBenchmarkActor._

  def receive = {
    case SendFile(file, connection, blockSize) =>
      val t0 = System.nanoTime()
      val size = new File(file).length().toInt
      val command = WriteFile(file,0,size,Finished)
      connection ! command
      context.become {
        case Finished =>
          printResult(size, System.nanoTime() - t0)
          connection ! Close
        case msg@CommandFailed(cmd) =>
          log.debug(msg.toString)
        case PeerClosed | Closed =>
          context.stop(self)
        case msg =>
          log.debug(msg.toString)
      }
  }
}

object WriteFileBenchmarkActor {
  case object Finished extends Event
}