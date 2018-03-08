package org.apache.spark.eventhubscommon.utils

import scala.concurrent.duration.{Deadline, Duration, DurationLong}
import scala.concurrent._

object Retry {

  /**
    * exponential back off for retry
    */
  def exponentialBackoff(r: Int): Duration = scala.math.pow(2, r).round * 100 milliseconds

  def noIgnore(t: Throwable): Boolean = false

  /**
    * retry a particular block that can fail
    *
    * @param maxRetry  how many times to retry before to giveup
    * @param deadline	how long to retry before giving up; default None
    * @param backoff		a back-off function that returns a Duration after which to retry. default is an exponential backoff at 100 milliseconds steps
    * @param ignoreThrowable		if you want to stop retrying on a particular exception
    * @param block	a block of code to retry
    * @param ctx	an execution context where to execute the block
    * returns	an eventual Future succeeded with the value computed or failed with one of:
    *   `TooManyRetriesException`	if there were too many retries without an exception being caught. Probably impossible if you pass decent parameters
    *   `DeadlineExceededException` if the retry didn't succeed before the provided deadline
    *   `TimeoutException`	if you provide a deadline and the block takes too long to execute
    *   `Throwable`	the last encountered exception
    */
  def retry[T](maxRetry: Int,
               deadline: Option[Deadline] = None,
               backoff: (Int) => Duration = exponentialBackoff,
               ignoreThrowable: Throwable => Boolean = noIgnore)(block: => T)(implicit ctx: ExecutionContext): Future[T] = {

    class TooManyRetriesException extends Exception("too many retries without exception")
    class DeadlineExceededException extends Exception("deadline exceeded")

    val p = Promise[T]()

    def recursiveRetry(retryCnt: Int, exception: Option[Throwable])(f: () => T): Option[T] = {
      if (maxRetry == retryCnt
        || deadline.isDefined && deadline.get.isOverdue) {
        exception match {
          case Some(t) =>
            p failure t
          case None if deadline.isDefined && deadline.get.isOverdue =>
            p failure new DeadlineExceededException
          case None =>
            p failure new TooManyRetriesException
        }
        None
      } else {
        val success = try {
          val rez = if (deadline.isDefined) {
            Await.result(Future{f()}, deadline.get.timeLeft)
          } else {
            f()
          }
          Some(rez)
        } catch {
          case t: Throwable if !ignoreThrowable(t) =>
            blocking {
              val interval = backoff(retryCnt).toMillis
              Thread.sleep(interval)
            }
            recursiveRetry(retryCnt + 1, Some(t))(f)
          case t: Throwable =>
            p failure t
            None
        }
        success match {
          case Some(v) =>
            p success v
            Some(v)
          case None => None
        }
      }
    }

    def doBlock() = block

    Future {
      recursiveRetry(0, None)(doBlock)
    }

    p.future
  }

}