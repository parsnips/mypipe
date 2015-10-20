package mypipe.avro

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

import mypipe.api.event.{ Serializer, AlterEvent, Mutation }
import mypipe.avro.schema.GenericSchemaRepository
import org.apache.avro.Schema
import org.apache.avro.generic.{ GenericRecord, GenericDatumWriter, GenericData }
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificRecord
import org.slf4j.LoggerFactory

/** Created by mparsons on 10/19/15.
 */
trait AvroRecordBuilder[SchemaId] {

  type InputRecord = SpecificRecord
  type OutputType = Array[Byte]

  val schemaRepoClient: GenericSchemaRepository[SchemaId, Schema]
  protected val serializer: Serializer[InputRecord, OutputType]

  protected val logger = LoggerFactory.getLogger(getClass)
  protected val encoderFactory = EncoderFactory.get()

  def handleAlter(event: AlterEvent): Boolean

  /** Builds the Kafka topic using the mutation's database, table name, and
   *  the mutation type (insert, update, delete) concatenating them with "_"
   *  as a delimeter.
   *
   *  @param mutation
   *  @return the topic name
   */
  def getTopic(mutation: Mutation): String

  /** Given a mutation, returns the "subject" that this mutation's
   *  Schema is registered under in the Avro schema repository.
   *  @param mutation
   *  @return
   */
  def avroSchemaSubject(mutation: Mutation): String

  /** Given a schema ID of type SchemaId, converts it to a byte array.
   *
   *  @param schemaId
   *  @return
   */
  def schemaIdToByteArray(schemaId: SchemaId): Array[Byte]

  /** Given a Mutation, this method must convert it into a(n) Avro record(s)
   *  for the given Avro schema.
   *
   *  @param mutation
   *  @param schema
   *  @return the Avro generic record(s)
   */
  def avroRecord(mutation: Mutation, schema: Schema): List[GenericData.Record]

  /** Adds a header into the given Record based on the Mutation's
   *  database, table, and tableId.
   *  @param record
   *  @param mutation
   */
  protected def header(record: GenericData.Record, mutation: Mutation) {
    record.put("database", mutation.table.db)
    record.put("table", mutation.table.name)
    record.put("tableId", mutation.table.id)

    // TODO: avoid null check
    if (mutation.txid != null && record.getSchema().getField("txid") != null) {
      val uuidBytes = ByteBuffer.wrap(new Array[Byte](16))
      uuidBytes.putLong(mutation.txid.getMostSignificantBits)
      uuidBytes.putLong(mutation.txid.getLeastSignificantBits)
      record.put("txid", new GenericData.Fixed(Guid.getClassSchema, uuidBytes.array))
    }
  }

  def magicByte(mutation: Mutation): Byte = Mutation.getMagicByte(mutation)

  def serialize(record: GenericData.Record, schema: Schema, schemaId: SchemaId, mutationType: Byte): Array[Byte] = {
    val encoderFactory = EncoderFactory.get()
    val writer = new GenericDatumWriter[GenericRecord]()
    writer.setSchema(schema)
    val out = new ByteArrayOutputStream()
    out.write(0x0.toByte)
    out.write(mutationType)
    out.write(schemaIdToByteArray(schemaId))
    val enc = encoderFactory.binaryEncoder(out, null)
    writer.write(record, enc)
    enc.flush
    out.toByteArray
  }

}
