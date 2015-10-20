package mypipe.producer

import com.typesafe.config.Config
import mypipe.avro.MutationGenericAvroRecordBuilder

//Backward Compatability
object KafkaMutationGenericAvroProducer {
  def apply(config: Config) = new KafkaMutationGenericAvroProducer(config)
}

/** An implementation of the base KafkaMutationAvroProducer class that uses a
 *  GenericInMemorySchemaRepo in order to encode mutations as Avro beans.
 *  Three beans are encoded: mypipe.avro.InsertMutation, UpdateMutation, and
 *  DeleteMutation. The Kafka topic names are calculated as:
 *  dbName_tableName_(insert|update|delete)
 *
 *  @param config configuration must have "metadata-brokers"
 */
class KafkaMutationGenericAvroProducer(config: Config)
    extends KafkaMutationAvroProducer[Short](config, new MutationGenericAvroRecordBuilder) {
}
