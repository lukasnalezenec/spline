/*
 * Copyright 2017 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.spline.migrator

import java.util.UUID

import akka.actor.Actor
import akka.pattern.pipe
import za.co.absa.spline.migrator.ArangoActor._
import za.co.absa.spline.model.DataLineage
import za.co.absa.spline.persistence.{ArangoFactory, Persister}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

object ArangoActor {

  trait RequestMessage

  case class LineagePersistRequest(lineage: DataLineage, streamed: Boolean = false) extends RequestMessage


  trait ResponseMessage

  abstract class LineagePersistResponse(val result: Try[Unit], val streamed: Boolean) extends ResponseMessage

  case class LineagePersistSuccess(override val streamed: Boolean) extends LineagePersistResponse(Success({}), streamed: Boolean)

  case class LineagePersistFailure(dsId: UUID, override val streamed: Boolean, e: RuntimeException)
    extends LineagePersistResponse(Failure(new LineagePersistException(dsId, streamed, e)), streamed)

  class LineagePersistException(val dsId: UUID, streamed: Boolean, e: RuntimeException)
    extends Exception(s"Failed to save ${if (streamed) "new streamed" else "" } lineage with id: $dsId", e)

}

class ArangoActor(connectionUrl: String) extends Actor {

  private val persister: Persister = Persister.create(connectionUrl)

  override def receive: Receive = {
    case LineagePersistRequest(lineage, streamed) =>
      save(lineage).
        map(_ => LineagePersistSuccess).
        recover({
          case e: RuntimeException => LineagePersistFailure(lineage.rootDataset.id, streamed, e)
        }).
        pipeTo(sender)
  }

  private def save(lineage: DataLineage): Future[Unit] = {
    println("Save lineage: " + lineage.id)
    persister.save(lineage)
  }
}
