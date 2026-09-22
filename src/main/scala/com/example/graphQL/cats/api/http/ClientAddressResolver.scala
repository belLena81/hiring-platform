package com.example.graphQL.cats.api.http

import com.comcast.ip4s.{Cidr, IpAddress}
import com.example.graphQL.cats.config.TrustedProxyConfig
import org.http4s.Request
import org.http4s.headers.{Forwarded, `X-Forwarded-For`}

final class ClientAddressResolver private (trustedProxyCidrs: List[Cidr[IpAddress]]) {
  def resolve[F[_]](request: Request[F]): Option[IpAddress] =
    request.remoteAddr.flatMap { peer =>
      if (!isTrusted(peer)) Some(peer)
      else forwardedAddress(request).orElse(xForwardedForAddress(request)).orElse(Some(peer))
    }

  private def isTrusted(address: IpAddress): Boolean =
    trustedProxyCidrs.exists(_.contains(address))

  private def forwardedAddress[F[_]](request: Request[F]): Option[IpAddress] =
    request.headers.get[Forwarded].flatMap { header =>
      firstClientAddress(header.values.toList.reverseIterator.map(concreteAddress))
    }

  private def xForwardedForAddress[F[_]](request: Request[F]): Option[IpAddress] =
    request.headers.get[`X-Forwarded-For`].flatMap { header =>
      firstClientAddress(header.values.toList.reverseIterator)
    }

  private def firstClientAddress(addresses: Iterator[Option[IpAddress]]): Option[IpAddress] =
    addresses.flatten.find(address => !isTrusted(address))

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
