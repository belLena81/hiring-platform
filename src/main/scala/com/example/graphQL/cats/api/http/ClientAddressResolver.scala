package com.example.graphQL.cats.api.http

import cats.syntax.all.*
import com.comcast.ip4s.{Cidr, IpAddress}
import com.example.graphQL.cats.config.TrustedProxyConfig
import org.http4s.Request
import org.http4s.headers.Forwarded

final class ClientAddressResolver private (trustedProxyCidrs: List[Cidr[IpAddress]]) {
  def resolve[F[_]](request: Request[F]): String =
    request.remoteAddr.fold("unknown") { peer =>
      if (!isTrusted(peer)) peer.toString
      else forwardedAddress(request).fold(peer)(identity).toString
    }

  private def isTrusted(address: IpAddress): Boolean =
    trustedProxyCidrs.exists(_.contains(address))

  private def forwardedAddress[F[_]](request: Request[F]): Option[IpAddress] =
    request.headers.get[Forwarded].flatMap { header =>
      header.values.toList.traverse(concreteAddress).flatMap(_.findLast(address => !isTrusted(address)))
    }

  private def concreteAddress(element: Forwarded.Element): Option[IpAddress] =
    element.maybeFor.flatMap { node =>
      node.nodeName match {
        case Forwarded.Node.Name.Ipv4(address) => Some(address)
        case Forwarded.Node.Name.Ipv6(address) => Some(address)
        case _ => None
      }
    }
}

object ClientAddressResolver {
  def apply(config: TrustedProxyConfig): ClientAddressResolver =
    new ClientAddressResolver(config.cidrs)
}
