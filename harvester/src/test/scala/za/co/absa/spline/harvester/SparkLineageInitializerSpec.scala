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

package za.co.absa.spline.harvester

import org.apache.commons.configuration.{BaseConfiguration, Configuration}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.util.{ExecutionListenerManager, QueryExecutionListener}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.Matchers._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FunSpec, Matchers}
import za.co.absa.spline.fixture.SparkFixture
import za.co.absa.spline.harvester.SparkLineageInitializer._
import za.co.absa.spline.harvester.SparkLineageInitializerSpec._
import za.co.absa.spline.harvester.conf.DefaultSplineConfigurer.ConfProperty._
import za.co.absa.spline.harvester.conf.SplineConfigurer.SplineMode._
import za.co.absa.spline.harvester.conf.{DefaultSplineConfigurer, LineageDispatcher}
import za.co.absa.spline.harvester.listener.SplineQueryExecutionListener
import za.co.absa.spline.persistence.api.{DataLineageReader, DataLineageWriter, PersistenceFactory, ProgressEventWriter}

object SparkLineageInitializerSpec {

  class MockReadWritePersistenceFactory(conf: Configuration) extends PersistenceFactory(conf) with MockitoSugar {
    override val createDataLineageWriter: DataLineageWriter = mock[DataLineageWriter]
    override val createDataLineageReader: Option[DataLineageReader] = Some(mock[DataLineageReader])
    override val createProgressEventWriter: ProgressEventWriter = mock[ProgressEventWriter]
  }

  class MockWriteOnlyPersistenceFactory(conf: Configuration) extends PersistenceFactory(conf) with MockitoSugar {
    override val createDataLineageWriter: DataLineageWriter = mock[DataLineageWriter]
    override val createDataLineageReader: Option[DataLineageReader] = None
    override val createProgressEventWriter: ProgressEventWriter = mock[ProgressEventWriter]
  }

  class MockFailingPersistenceFactory(conf: Configuration) extends PersistenceFactory(conf) with MockitoSugar {
    override val createDataLineageWriter: DataLineageWriter = sys.error("boom!")
    override val createDataLineageReader: Option[DataLineageReader] = sys.error("bam!")
    override val createProgressEventWriter: ProgressEventWriter = sys.error("bim!")
  }

  private[this] val getSparkQueryExecutionListenerClasses: () => Seq[Class[_ <: QueryExecutionListener]] = {
    val listenersField = classOf[ExecutionListenerManager] getDeclaredField "org$apache$spark$sql$util$ExecutionListenerManager$$listeners"
    listenersField setAccessible true
    () =>
      for {
        session <- SparkSession.getActiveSession.toSeq
        listenerManager = session.listenerManager.clone // to avoid potential race on mutable field
        listener <- (listenersField get listenerManager).asInstanceOf[Seq[QueryExecutionListener]]
      } yield listener.getClass
  }

  private def assertSplineIsEnabled() = getSparkQueryExecutionListenerClasses() should contain(classOf[SplineQueryExecutionListener])

  private def assertSplineIsDisabled() = getSparkQueryExecutionListenerClasses() should not contain classOf[SplineQueryExecutionListener]
}

class SparkLineageInitializerSpec extends FunSpec with BeforeAndAfterEach with Matchers with SparkFixture {
  private val configuration = new BaseConfiguration
  configuration.setProperty("spark.master", "local")

  describe("defaultConfiguration") {

    val keyDefinedEverywhere = "key.defined.everywhere"
    val keyDefinedInHadoopAndSpline = "key.defined.in_Hadoop_and_Spline"
    val keyDefinedInHadoopAndSpark = "key.defined.in_Hadoop_and_Spark"
    val keyDefinedInSparkAndSpline = "key.defined.in_Spark_and_Spline"
    val keyDefinedInJVMAndSpline = "key.defined.in_JVM_and_Spline"
    val keyDefinedInSplineOnly = "key.defined.in_Spline_only"

    val valueFromHadoop = "value from Hadoop configuration"
    val valueFromSpark = "value from Spark configuration"
    val valueFromJVM = "value from JVM args"
    val valueFromSpline = "value from spline.properties"

    it("should look through the multiple sources for the configuration properties") {
      withNewSession(sparkSession => {
        val jvmProps = System.getProperties
        //      jvmProps.setProperty("spark.master", "local")

        val sparkConf = (classOf[SparkContext] getMethod "conf" invoke sparkSession.sparkContext).asInstanceOf[SparkConf]
        val hadoopConf = sparkSession.sparkContext.hadoopConfiguration

        for (key <- Some(keyDefinedEverywhere)) {
          jvmProps.setProperty(key, valueFromJVM)
          hadoopConf.set(key, valueFromHadoop)
          sparkConf.set(s"spark.$key", valueFromSpark)
          // skip setting spline prop as it's already hardcoded in spline.properties
        }

        for (key <- Some(keyDefinedInJVMAndSpline)) {
          jvmProps.setProperty(key, valueFromJVM)
          // skip setting spline prop as it's already hardcoded in spline.properties
        }

        for (key <- Some(keyDefinedInHadoopAndSpline)) {
          hadoopConf.set(key, valueFromHadoop)
          // skip setting spline prop as it's already hardcoded in spline.properties
        }

        for (key <- Some(keyDefinedInHadoopAndSpark)) {
          hadoopConf.set(key, valueFromHadoop)
          sparkConf.set(s"spark.$key", valueFromSpark)
        }

        for (key <- Some(keyDefinedInSparkAndSpline)) {
          sparkConf.set(s"spark.$key", valueFromSpark)
          // skip setting spline prop as it's already hardcoded in spline.properties
        }

        val splineConfiguration = sparkSession.defaultSplineConfiguration

        splineConfiguration getString keyDefinedEverywhere shouldEqual valueFromHadoop
        splineConfiguration getString keyDefinedInJVMAndSpline shouldEqual valueFromJVM
        splineConfiguration getString keyDefinedInHadoopAndSpline shouldEqual valueFromHadoop
        splineConfiguration getString keyDefinedInHadoopAndSpark shouldEqual valueFromHadoop
        splineConfiguration getString keyDefinedInSparkAndSpline shouldEqual valueFromSpark
        splineConfiguration getString keyDefinedInSplineOnly shouldEqual valueFromSpline
        splineConfiguration getString "key.undefined" shouldBe null
      })
    }
  }

  describe("enableLineageTracking()") {
    it("should not allow double initialization") {
      intercept[IllegalStateException] {
        withNewSession(session =>
          session
            .enableLineageTracking(createConfigurer(session)) // 1st is fine
            .enableLineageTracking(createConfigurer(session)) // 2nd should fail
        )
      }
    }

    it("should return the spark session back to the caller") {
      withNewSession(session =>
        session.enableLineageTracking(createConfigurer(session)) shouldBe session
      )
    }

    describe("modes") {

      it("should disable Spline and proceed, when is in BEST_EFFORT (default) mode") {
        withNewSession(sparkSession => {
          configuration.setProperty(MODE, BEST_EFFORT.toString)
          sparkSession.enableLineageTracking(createFailingConfigurer(sparkSession))
          assertSplineIsDisabled()
        })
      }

      it("should disable Spline and proceed, when is in DEFAULT mode") {
        withNewSession(sparkSession => {
          configuration.clearProperty(MODE) // default mode is BEST_EFFORT
          sparkSession.enableLineageTracking(createFailingConfigurer(sparkSession))
          assertSplineIsDisabled()
        })
      }

      it("should abort application, when is in REQUIRED mode") {
        intercept[Exception] {
          withNewSession(session => {
            configuration.setProperty(MODE, REQUIRED.toString)
            session.enableLineageTracking(createFailingConfigurer(session))
          })
        }
      }

      it("should have no effect, when is in DISABLED mode") {
        withNewSession(sparkSession => {
          configuration.setProperty(MODE, DISABLED.toString)
          sparkSession.enableLineageTracking(createFailingConfigurer(sparkSession))
          assertSplineIsDisabled()
        })
      }

      def createFailingConfigurer(sparkSession: SparkSession) = {
        new DefaultSplineConfigurer(configuration, sparkSession) {
          override lazy val lineageDispatcher: LineageDispatcher = {
            throw new RuntimeException("Testing exception - please ignore.")
          }
        }
      }

    }
  }

  def createConfigurer(sparkSession: SparkSession): DefaultSplineConfigurer = {
    new DefaultSplineConfigurer(configuration, sparkSession)
  }

}
