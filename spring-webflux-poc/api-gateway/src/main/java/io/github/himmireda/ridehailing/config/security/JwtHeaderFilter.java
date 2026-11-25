package io.github.himmireda.ridehailing.config.security;

import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.http.HttpHeaders;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * Turns the verified JWT into identity headers for the downstream services.
 *
 * <p>Downstream services trust these headers without checking anything, so the gateway must be the
 * only one that sets them: every incoming copy is removed first, on every route, and the values are
 * then set (not added) from the token's claims.
 */
@Component
public class JwtHeaderFilter implements GlobalFilter {

    static final String USER_ID = "X-User-Id";
    static final String ROLE = "X-Role";
    static final String EMAIL = "X-Email";
    static final String FIRST_NAME = "X-First-Name";
    static final String LAST_NAME = "X-Last-Name";
    static final String PHONE_NUMBER = "X-Phone-Number";

    static final List<String> IDENTITY_HEADERS = List.of(USER_ID, ROLE, EMAIL, FIRST_NAME, LAST_NAME, PHONE_NUMBER);

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        // HttpHeaders names are case-insensitive, so "x-user-id" is removed too.
        ServerHttpRequest stripped = exchange.getRequest().mutate()
                .headers(headers -> IDENTITY_HEADERS.forEach(headers::remove))
                .build();
        ServerWebExchange strippedExchange = exchange.mutate().request(stripped).build();

        String path = stripped.getPath().value();
        if (path.startsWith("/api/users")) {
            return chain.filter(strippedExchange);
        }

        return exchange.getPrincipal()
                .cast(Authentication.class)
                .map(Authentication::getPrincipal)
                .filter(Jwt.class::isInstance)
                .cast(Jwt.class)
                .map(jwt -> strippedExchange.mutate()
                        .request(stripped.mutate().headers(headers -> setIdentityHeaders(headers, jwt)).build())
                        .build())
                .defaultIfEmpty(strippedExchange)
                .flatMap(chain::filter);
    }

    private static void setIdentityHeaders(HttpHeaders headers, Jwt jwt) {
        headers.set(USER_ID, jwt.getSubject());
        headers.set(ROLE, String.valueOf(jwt.getClaims().get("role")));
        headers.set(EMAIL, String.valueOf(jwt.getClaims().get("email")));
        headers.set(FIRST_NAME, String.valueOf(jwt.getClaims().get("firstName")));
        headers.set(LAST_NAME, String.valueOf(jwt.getClaims().get("lastName")));
        headers.set(PHONE_NUMBER, String.valueOf(jwt.getClaims().get("phoneNumber")));
    }
}
