package mypipe.producer

import com.typesafe.config.Config
import mypipe.avro.MutationSpecificAvroRecordBuilder

// Backwards compatible
class KafkaMutationSpecificAvroProducer(config: Config)
  extends KafkaMutationAvroProducer[Short](config, new MutationSpecificAvroRecordBuilder(config))
