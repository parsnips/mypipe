package mypipe.avro

import com.typesafe.config.Config
import mypipe.api.{ event ⇒ api }
//import mypipe.api.event.{UpdateMutation, SingleValuedMutation, Mutation, AlterEvent}
import mypipe.avro.schema.{ AvroSchemaUtils, GenericSchemaRepository }
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

/** Created by mparsons on 10/19/15.
 */
class MutationSpecificAvroRecordBuilder(config: Config)
    extends AvroRecordBuilder[Short] {
  private val schemaRepoClientClassName = config.getString("schema-repo-client")

  val schemaRepoClient = Class.forName(schemaRepoClientClassName + "$").getField("MODULE$").get(null).asInstanceOf[GenericSchemaRepository[Short, Schema]]

  override protected val serializer = new AvroVersionedRecordSerializer[InputRecord](schemaRepoClient)

  override def handleAlter(event: api.AlterEvent): Boolean = {
    // FIXME: if the table is not in the cache already, by it's ID, this will fail
    // refresh insert, update, and delete schemas
    (for (
      i ← schemaRepoClient.getLatestSchema(AvroSchemaUtils.specificSubject(event.database, event.table.name, api.Mutation.InsertString), flushCache = true);
      u ← schemaRepoClient.getLatestSchema(AvroSchemaUtils.specificSubject(event.database, event.table.name, api.Mutation.UpdateString), flushCache = true);
      d ← schemaRepoClient.getLatestSchema(AvroSchemaUtils.specificSubject(event.database, event.table.name, api.Mutation.DeleteString), flushCache = true)
    ) yield {
      true
    }).getOrElse(false)
  }

  override def schemaIdToByteArray(s: Short) = Array[Byte](((s & 0xFF00) >> 8).toByte, (s & 0x00FF).toByte)

  override def getTopic(mutation: api.Mutation): String = TopicUtil.specificTopic(mutation)

  override def avroRecord(mutation: api.Mutation, schema: Schema): List[GenericData.Record] = {

    api.Mutation.getMagicByte(mutation) match {
      case api.Mutation.InsertByte ⇒ insertOrDeleteMutationToAvro(mutation.asInstanceOf[api.SingleValuedMutation], schema)
      case api.Mutation.UpdateByte ⇒ updateMutationToAvro(mutation.asInstanceOf[api.UpdateMutation], schema)
      case api.Mutation.DeleteByte ⇒ insertOrDeleteMutationToAvro(mutation.asInstanceOf[api.SingleValuedMutation], schema)
      case _ ⇒
        logger.error(s"Unexpected mutation type ${mutation.getClass} encountered; retuning empty Avro GenericData.Record(schema=$schema")
        List(new GenericData.Record(schema))
    }
  }

  /** Given a mutation, returns the Avro subject that this mutation's
   *  Schema is registered under in the Avro schema repository.
   *
   *  @param mutation mutation to get subject for
   *  @return returns "mutationDbName_mutationTableName_mutationType" where mutationType is "insert", "update", or "delete"
   */
  override def avroSchemaSubject(mutation: api.Mutation): String = AvroSchemaUtils.specificSubject(mutation)

  protected def insertOrDeleteMutationToAvro(mutation: api.SingleValuedMutation, schema: Schema): List[GenericData.Record] = {

    mutation.rows.map(row ⇒ {
      val record = new GenericData.Record(schema)
      row.columns.foreach(col ⇒ Option(schema.getField(col._1)).foreach(f ⇒ record.put(f.name(), col._2.value)))
      header(record, mutation)
      record
    })
  }

  protected def updateMutationToAvro(mutation: api.UpdateMutation, schema: Schema): List[GenericData.Record] = {

    mutation.rows.map(row ⇒ {
      val record = new GenericData.Record(schema)
      row._1.columns.foreach(col ⇒ Option(schema.getField("old_" + col._1)).foreach(f ⇒ record.put(f.name(), col._2.value)))
      row._2.columns.foreach(col ⇒ Option(schema.getField("new_" + col._1)).foreach(f ⇒ record.put(f.name(), col._2.value)))
      header(record, mutation)
      record
    })
  }
}
