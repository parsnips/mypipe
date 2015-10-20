package mypipe.avro

import java.util.HashMap

import mypipe.api.data.{ ColumnType, Column }
import mypipe.api.event._
import mypipe.api.event
import mypipe.avro.schema.AvroSchemaUtils
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericData.Record

import java.lang.{ Long ⇒ JLong }
import java.util.{ HashMap ⇒ JMap }

/** Created by mparsons on 10/19/15.
 */
class MutationGenericAvroRecordBuilder extends AvroRecordBuilder[Short] {
  val schemaRepoClient = GenericInMemorySchemaRepo
  protected val serializer = new AvroVersionedRecordSerializer[InputRecord](schemaRepoClient)

  def handleAlter(event: AlterEvent): Boolean = true

  /** Builds the Kafka topic using the mutation's database, table name, and
   *  the mutation type (insert, update, delete) concatenating them with "_"
   *  as a delimeter.
   *
   *  @param mutation
   *  @return the topic name
   */
  def getTopic(mutation: Mutation): String = TopicUtil.genericTopic(mutation)

  /** Given a mutation, returns the "subject" that this mutation's
   *  Schema is registered under in the Avro schema repository.
   *  @param mutation
   *  @return
   */
  def avroSchemaSubject(mutation: Mutation): String = AvroSchemaUtils.genericSubject(mutation)

  /** Given a schema ID of type SchemaId, converts it to a byte array.
   *
   *  @param s
   *  @return
   */
  def schemaIdToByteArray(s: Short): Array[Byte] = Array[Byte](((s & 0xFF00) >> 8).toByte, (s & 0x00FF).toByte)

  /** Given a Mutation, this method must convert it into a(n) Avro record(s)
   *  for the given Avro schema.
   *
   *  @param mutation
   *  @param schema
   *  @return the Avro generic record(s)
   */
  def avroRecord(mutation: Mutation, schema: Schema): List[Record] = {

    Mutation.getMagicByte(mutation) match {

      case Mutation.InsertByte ⇒ mutation.asInstanceOf[event.InsertMutation].rows.map(row ⇒ {
        val (integers, strings, longs) = columnsToMaps(row.columns)
        val record = new GenericData.Record(schema)
        header(record, mutation)
        body(record, mutation, integers, strings, longs)
        record
      })

      case Mutation.DeleteByte ⇒ mutation.asInstanceOf[event.DeleteMutation].rows.map(row ⇒ {
        val (integers, strings, longs) = columnsToMaps(row.columns)
        val record = new GenericData.Record(schema)
        header(record, mutation)
        body(record, mutation, integers, strings, longs)
        record
      })

      case Mutation.UpdateByte ⇒ mutation.asInstanceOf[event.UpdateMutation].rows.map(row ⇒ {
        val (integersOld, stringsOld, longsOld) = columnsToMaps(row._1.columns)
        val (integersNew, stringsNew, longsNew) = columnsToMaps(row._2.columns)
        val record = new GenericData.Record(schema)
        header(record, mutation)
        body(record, mutation, integersOld, stringsOld, longsOld) { s ⇒ "old_" + s }
        body(record, mutation, integersNew, stringsNew, longsNew) { s ⇒ "new_" + s }
        record
      })

      case _ ⇒
        logger.error(s"Unexpected mutation type ${mutation.getClass} encountered; retuning empty Avro GenericData.Record(schema=$schema")
        List(new GenericData.Record(schema))
    }
  }

  protected def body(
    record:   GenericData.Record,
    mutation: Mutation,
    integers: JMap[CharSequence, Integer],
    strings:  JMap[CharSequence, CharSequence],
    longs:    JMap[CharSequence, JLong]
  )(implicit keyOp: String ⇒ String = s ⇒ s) {
    record.put(keyOp("integers"), integers)
    record.put(keyOp("strings"), strings)
    record.put(keyOp("longs"), longs)
  }

  protected def columnsToMaps(columns: Map[String, Column]): (JMap[CharSequence, Integer], JMap[CharSequence, CharSequence], JMap[CharSequence, JLong]) = {

    val cols = columns.values.groupBy(_.metadata.colType)

    // ugliness follows... we'll clean it up some day.
    val integers = new java.util.HashMap[CharSequence, Integer]()
    val strings = new java.util.HashMap[CharSequence, CharSequence]()
    val longs = new java.util.HashMap[CharSequence, java.lang.Long]()

    cols.foreach({

      case (ColumnType.INT24, colz) ⇒
        colz.foreach(c ⇒ {
          val v = c.valueOption[Int]
          if (v.isDefined) integers.put(c.metadata.name, v.get)
        })

      case (ColumnType.VARCHAR, colz) ⇒
        colz.foreach(c ⇒ {
          val v = c.valueOption[String]
          if (v.isDefined) strings.put(c.metadata.name, v.get)
        })

      case (ColumnType.LONG, colz) ⇒
        colz.foreach(c ⇒ {
          // this damn thing can come in as an Integer or Long
          val v = c.value match {
            case i: java.lang.Integer ⇒ new java.lang.Long(i.toLong)
            case l: java.lang.Long    ⇒ l
            case null                 ⇒ null
          }

          if (v != null) longs.put(c.metadata.name, v)
        })

      case _ ⇒ // unsupported
    })

    (integers, strings, longs)
  }

}
