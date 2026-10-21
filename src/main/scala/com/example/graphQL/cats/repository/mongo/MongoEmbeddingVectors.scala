package com.example.graphQL.cats.repository.mongo

import org.bson.{BinaryVector, BsonBinary, BsonBinarySubType}
import org.bson.types.Binary
import scala.util.Try

/** The stored form of an entity embedding: a BSON binary vector (binData subtype 9) of float32 elements. The domain
  * type is `Float`, so the stored form is exact; the float64 array it replaced only doubled the bytes (migration
  * `028_binary_embedding_vectors`). Decoding is total: any other stored form is `None` and the caller fails closed.
  */
private[mongo] object MongoEmbeddingVectors {
  def encode(values: List[Float]): BinaryVector = BinaryVector.floatVector(values.toArray)

  def decode(stored: Any): Option[List[Float]] = stored match {
    case vector: BinaryVector => floats(vector)
    case binary: BsonBinary   => fromBinary(binary.getType, binary.getData)
    case binary: Binary       => fromBinary(binary.getType, binary.getData)
    case _                    => None
  }

  private def fromBinary(subType: Byte, data: Array[Byte]): Option[List[Float]] =
    if (subType != BsonBinarySubType.VECTOR.getValue) None
    else Try(new BsonBinary(subType, data).asVector()).toOption.flatMap(floats)

  private def floats(vector: BinaryVector): Option[List[Float]] =
    if (vector.getDataType == BinaryVector.DataType.FLOAT32) Some(vector.asFloat32Vector().getData.toList) else None
}
