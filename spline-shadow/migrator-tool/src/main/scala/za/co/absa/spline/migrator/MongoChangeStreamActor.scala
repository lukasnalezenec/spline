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

import akka.actor.{Actor, ActorLogging, ActorRef, Status}
import akka.pattern.pipe
import com.mongodb.client.MongoCollection
import org.apache.commons.configuration.BaseConfiguration
import za.co.absa.spline.common.ARM.managed
import za.co.absa.spline.migrator.MigratorActor.Start
import za.co.absa.spline.migrator.MongoActor._
import za.co.absa.spline.model.{DataLineage, PersistedDatasetDescriptor}
import za.co.absa.spline.persistence.api.CloseableIterable
import za.co.absa.spline.persistence.mongo.{MongoConnectionImpl, MongoPersistenceFactory}
import za.co.absa.spline.persistence.mongo.MongoPersistenceFactory._

import scala.concurrent.ExecutionContext


class MongoChangeStreamActor(connectionUrl: String) extends Actor with ActorLogging {
  implicit val ec: ExecutionContext = context.dispatcher

  private val mongoConnection = new MongoConnectionImpl(connectionUrl)

  // FIXME add change stream subscription
//  private val mongoReader =
//    new MongoPersistenceFactory(
//      new BaseConfiguration {
//        addProperty(MongoPersistenceFactory.mongoDbUrlKey, connectionUrl)
//      }).
//      createDataLineageReader.
//      get

  override def receive: Receive = {
    case Start =>
      mongoConnection.db.getCollection("").asInstanceOf[Mong]
    case GetLineages(page) =>
      this.receive
//      val pageProcessor = pipePageTo(sender) _
//      mongoReader.
//        findDatasets(None, page).
//        map(managed(pageProcessor)).
//        pipeTo(self)

//    case PostTo(message, receiver) =>
//      receiver ! message

    case Status.Failure(e) => throw e
  }

}


case class StreamedLineage(dataLineage: DataLineage)
