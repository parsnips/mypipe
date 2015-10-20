package mypipe.producer

import java.nio.ByteBuffer

import mypipe.api._
import mypipe.api.event.{ AlterEvent, Serializer, Mutation }
import mypipe.api.producer.Producer
import mypipe.avro.{ AvroRecordBuilder, Guid }
import mypipe.kafka.KafkaProducer
import com.typesafe.config.Config
import mypipe.avro.schema.{ GenericSchemaRepository }
import org.apache.avro.specific.SpecificRecord
import org.apache.avro.Schema
import org.apache.avro.generic.{ GenericRecord, GenericDatumWriter, GenericData }
import org.apache.avro.io.EncoderFactory
import java.io.ByteArrayOutputStream
import mypipe.kafka.{ PROTO_MAGIC_V0 }
import org.slf4j.LoggerFactory

/** The base class for a Mypipe producer that encodes Mutation instances
 *  as Avro records and publishes them into Kafka.
 *
 *  @param config configuration must have "metadata-brokers"
 */
abstract class KafkaMutationAvroProducer[SchemaId](config: Config, val builder: AvroRecordBuilder[SchemaId])
    extends Producer(config = config) {

  protected val metadataBrokers = config.getString("metadata-brokers")
  protected val producer = new KafkaProducer[builder.OutputType](metadataBrokers)

  protected val logger = LoggerFactory.getLogger(getClass)
  protected val encoderFactory = EncoderFactory.get()

  def handleAlter(event: AlterEvent): Boolean = {
    builder.handleAlter(event)
  }

  override def flush(): Boolean = {
    try {
      producer.flush
      true
    } catch {
      case e: Exception ⇒ {
        logger.error(s"Could not flush producer queue: ${e.getMessage} -> ${e.getStackTraceString}")
        false
      }
    }
  }

  override def queueList(inputList: List[Mutation]): Boolean = {
    inputList.foreach(input ⇒ {
      val schemaTopic = builder.avroSchemaSubject(input)
      val mutationType = builder.magicByte(input)
      val schema = builder.schemaRepoClient.getLatestSchema(schemaTopic).get
      val schemaId = builder.schemaRepoClient.getSchemaId(schemaTopic, schema)
      val records = builder.avroRecord(input, schema)

      records foreach (record ⇒ {
        val bytes = builder.serialize(record, schema, schemaId.get, mutationType)
        producer.send(builder.getTopic(input), bytes)
      })
    })

    true
  }

  override def queue(input: Mutation): Boolean = {
    try {
      val schemaTopic = builder.avroSchemaSubject(input)
      val mutationType = builder.magicByte(input)
      val schema = builder.schemaRepoClient.getLatestSchema(schemaTopic).get
      val schemaId = builder.schemaRepoClient.getSchemaId(schemaTopic, schema)
      val records = builder.avroRecord(input, schema)

      records foreach (record ⇒ {
        val bytes = builder.serialize(record, schema, schemaId.get, mutationType)
        producer.send(builder.getTopic(input), bytes)
      })

      true
    } catch {
      case e: Exception ⇒ logger.error(s"failed to queue: ${e.getMessage}\n${e.getStackTraceString}"); false
    }
  }

  override def toString(): String = {
    s"kafka-avro-producer-$metadataBrokers"
  }

}

