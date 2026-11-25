package io.github.himmireda.ridehailing.config.security;

import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.Map;

@Component
public class JwtHeaderFilter implements GlobalFilter {

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String path = exchange.getRequest().getPath().value();
        if (path.startsWith("/api/users")) {
            return chain.filter(exchange);
        }

        return exchange.getPrincipal()
                .cast(Authentication.class)
                .flatMap(auth -> {
                    if (auth.getPrincipal() instanceof Jwt jwt) {
                        Map<String, Object> claims = jwt.getClaims();

                        ServerHttpRequest request = exchange.getRequest().mutate()
                                .header("X-User-Id", jwt.getSubject())
                                .header("X-Role", String.valueOf(claims.get("role")))
                                .header("X-Email", String.valueOf(claims.get("email")))
                                .header("X-First-Name", String.valueOf(claims.get("firstName")))
                                .header("X-Last-Name", String.valueOf(claims.get("lastName")))
                                .header("X-Phone-Number", String.valueOf(claims.get("phoneNumber")))
                                .build();

                        return chain.filter(exchange.mutate().request(request).build());
                    }
                    return chain.filter(exchange);
                })
                .switchIfEmpty(chain.filter(exchange));
    }
}

