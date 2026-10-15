package com.example.graphQL.cats

package object service {
  type DatabaseProbe[F[_]] = port.DatabaseProbe[F]
}
