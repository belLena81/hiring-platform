package com.example.hiring.analytics.adapter.spark
import com.example.hiring.analytics.service.keyretirement.*
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.errors.*
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.config.*
import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.adapter.mongo.*
import com.example.hiring.analytics.adapter.kafka.*
import com.example.hiring.analytics.adapter.local.*
import com.example.hiring.analytics.service.erasure.*

import scala.reflect.ClassTag
import scala.reflect.api.{Mirror, TypeCreator, Universe}
import scala.reflect.runtime.{universe => ru}

/** Supplies Spark's legacy Scala 2 runtime TypeTag for product encoders used from the Scala 3 driver. */
private[spark] object SparkProductTypeTag {
  def apply[A: ClassTag]: ru.TypeTag[A] = {
    val runtimeClass = summon[ClassTag[A]].runtimeClass
    val mirror = ru.runtimeMirror(runtimeClass.getClassLoader)
    ru.TypeTag[A](
      mirror,
      new TypeCreator {
        override def apply[U <: Universe & Singleton](value: Mirror[U]): value.universe.Type =
          value.staticClass(runtimeClass.getName).toType.asInstanceOf[value.universe.Type]
      }
    )
  }
}
