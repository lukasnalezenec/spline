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

package za.co.absa.spline.web.rest.controller

import java.util.UUID

import javax.servlet.http.HttpServletResponse
import org.apache.commons.lang.StringUtils.trimToNull
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.MediaType.APPLICATION_JSON_VALUE
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.RequestMethod._
import org.springframework.web.bind.annotation.{PathVariable, RequestMapping, RequestParam, ResponseBody}
import za.co.absa.spline.common.future.EstimableFuture
import za.co.absa.spline.persistence.api.DataLineageReader
import za.co.absa.spline.persistence.api.DataLineageReader.{IntervalPageRequest, PageRequest}
import za.co.absa.spline.web.ExecutionContextImplicit
import za.co.absa.spline.web.json.StringJSONConverters
import za.co.absa.spline.web.rest.service.LineageService

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

@Controller
@RequestMapping(
  method = Array(GET),
  produces = Array(APPLICATION_JSON_VALUE))
class LineageController @Autowired()
(
  val reader: DataLineageReader,
  val service: LineageService
) extends ExecutionContextImplicit with EstimableFuture.Implicits {

  import StringJSONConverters._

  @RequestMapping(Array("/dataset/descriptors"))
  def datasetDescriptors
  (
    @RequestParam(name = "q", required = false) text: String,
    @RequestParam(name = "asAtTime", required = false, defaultValue = "9223372036854775807") timestamp: Long,
    @RequestParam(name = "offset", required = false, defaultValue = "0") offset: Int,
    @RequestParam(name = "size", required = false, defaultValue = "2147483647") size: Int,
    @RequestParam(name = "from", required = false, defaultValue = "-1") from: Long,
    @RequestParam(name = "to", required = false, defaultValue = "-1") to: Long,
    response: HttpServletResponse
    ): EstimableFuture[Unit] = {
    val futureResult =
      reader.findDatasets(
        Option(trimToNull(text)),
        PageRequest(timestamp, offset, size))

    if (from == -1 || to == -1) {
      reader.findDatasets(
        Option(trimToNull(text)),
        PageRequest(timestamp, offset, size))
        .map(_ asJsonArrayInto response.getWriter)
        .asEstimable(category = s"lineage/descriptors:$size")
    } else {
      reader.findDatasets(
        Option(trimToNull(text)),
        IntervalPageRequest(from, to))
        .map(_ asJsonArrayInto response.getWriter)
        .asEstimable(category = s"lineage/descriptors:$size")
    }
  }


  @RequestMapping(Array("/dataset/{id}/descriptor"))
  @ResponseBody
  def datasetDescriptor(@PathVariable("id") id: UUID): EstimableFuture[String] =
    reader.getDatasetDescriptor(id).map(_.toJson).asEstimable(category = "lineage/descriptor")

  @RequestMapping(Array("/dataset/{id}/lineage/partial"))
  @ResponseBody
  def datasetLineage(@PathVariable("id") id: UUID): EstimableFuture[String] =
    reader.loadByDatasetId(id, overviewOnly = false).map(_.get.toJson).asEstimable(category = "lineage/partial")

  @RequestMapping(path = Array("/dataset/{id}/lineage/overview"), method = Array(GET))
  @ResponseBody
  def datasetLineageOverview(@PathVariable("id") id: UUID): EstimableFuture[String] =
    service.getPrelinked(id).map(_.toJson).asEstimable(category = "lineage/overview")

  // FIXME quiery with endpoint URI and not with any of corresponding datasets id.
  @RequestMapping(path = Array("/dataset/{id}/lineage/interval"), method = Array(GET))
  @ResponseBody
  def intervalLineageOverview(
                               @PathVariable("id") id: UUID,
                               @RequestParam(name = "from") from: Long,
                               @RequestParam(name = "to") to: Long): Future[String] = {

    service
      .getInterval(id, from, to)
      .map(_.toJson)
  }
}
