/*
 * Copyright Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.streams.zephyrstores

import java.lang.{Long => JLong}
import java.util.Properties

import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.kstream.{KStream, KTable, Produced}
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsBuilder, StreamsConfig}
import org.apache.kafka.test.TestUtils
import org.assertj.core.api.Assertions.assertThat
import org.junit._
import org.scalatest.junit.AssertionsForJUnit

class BasicZephyrUnitTest extends AssertionsForJUnit {

  private val inputTopic = "inputTopic"
  private val outputTopic = "output-topic"

  @Before
  def startKafkaCluster() {
  }

  @Test
  def shouldCountWords() {
    // To convert between Scala's `Tuple2` and Streams' `KeyValue`.

    val inputTextLines: Seq[String] = Seq(
      "Hello Kafka Streams",
      "All streams lead to Kafka",
      "Join Kafka Summit"
    )

/*
    val expectedWordCounts: Seq[KeyValue[String, Long]] = Seq(KeyValue
      ("hello", 1L),
      ("all", 1L),
      ("streams", 2L),
      ("lead", 1L),
      ("to", 1L),
      ("join", 1L),
      ("kafka", 3L),
      ("summit", 1L)
    )
*/

    import collection.JavaConverters._

    //val actualWordCounts: java.util.List[KeyValue[String, Long]] =

//    assertThat(actualWordCounts).containsExactlyElementsOf(expectedWordCounts.asJava)
  }

}
