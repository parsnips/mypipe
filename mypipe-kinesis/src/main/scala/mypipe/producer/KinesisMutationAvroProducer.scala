package mypipe.producer

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.kinesis.model._
import com.amazonaws.{ ClientConfiguration }
import com.amazonaws.services.kinesis.{ AmazonKinesisClient, AmazonKinesis }
import com.typesafe.config.Config
import mypipe.api.event.{ Serializer, AlterEvent, Mutation }
import mypipe.api.producer.Producer
import mypipe.avro.{ MutationGenericAvroRecordBuilder, AvroRecordBuilder }
import mypipe.avro.schema.GenericSchemaRepository
import org.apache.avro.Schema
import org.apache.avro.generic.{ GenericRecord, GenericDatumWriter, GenericData }
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificRecord
import org.apache.http.impl.client.BasicCredentialsProvider
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.util.{ Failure, Success, Try }

class KinesisMutationAvroProducer(config: Config)
    extends Producer(config = config) {

  val builder: MutationGenericAvroRecordBuilder = new MutationGenericAvroRecordBuilder

  protected val kinesis: AmazonKinesis = new AmazonKinesisClient(new BasicAWSCredentials(System.getenv("AWS_ACCESS_KEY_ID"), System.getenv("AWS_SECRET_ACCESS_KEY")))
  protected val logger = LoggerFactory.getLogger(getClass)

  def handleAlter(event: AlterEvent): Boolean = {
    //delegate
    builder.handleAlter(event)
  }

  override def queue(mutation: Mutation): Boolean = {
    logger.info("single mutation")
    queueList(List(mutation))
  }

  override def flush(): Boolean = {
    logger.info("flush called")
    true
  }

  override def queueList(mutation: List[Mutation]): Boolean = {
    logger.info("lots of mutations")
    mutation.foreach { mut ⇒

      val schemaTopic = builder.avroSchemaSubject(mut)
      logger.info(s"topic is $schemaTopic")
      val mutationType = Mutation.getMagicByte(mut)

      val latestSchema = builder.schemaRepoClient.getLatestSchema(schemaTopic)

      logger.info(s"latest schema: $latestSchema")

      latestSchema.foreach { schema ⇒

        val schemaId = builder.schemaRepoClient.getSchemaId(schemaTopic, schema)
        val putRequest = new PutRecordsRequest()

        putRequest.setStreamName(builder.getTopic(mut))

        val putEntryList = builder.avroRecord(mut, schema).map { record ⇒
          val bytes = builder.serialize(record, schema, schemaId.get, mutationType)
          val entry = new PutRecordsRequestEntry()
          entry.setData(ByteBuffer.wrap(bytes))
          entry.setPartitionKey(" ") // For now everyone gets the same partition
          entry
        }

        putRequest.setRecords(putEntryList.asJava)
        kinesis.putRecords(putRequest)
        logger.info(s"just put some records bitches.")
      }
    }

    true
  }
}
