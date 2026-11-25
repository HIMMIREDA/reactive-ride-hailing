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

        if (isPublicPath(stripped.getPath().value())) {
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

    /** Exactly "/api/users" and "/api/users/**": "/api/users-admin" or "/api/usersX" are not public. */
    static boolean isPublicPath(String path) {
        return path.equals("/api/users") || path.startsWith("/api/users/");
    }

    private static void setIdentityHeaders(HttpHeaders headers, Jwt jwt) {
        // sub, role and email are guaranteed by the decoder's validators (JwtDecoderConfig.REQUIRED_CLAIMS).
        setIfPresent(headers, USER_ID, jwt.getSubject());
        setIfPresent(headers, ROLE, jwt.getClaimAsString("role"));
        setIfPresent(headers, EMAIL, jwt.getClaimAsString("email"));
        // Optional profile claims: a missing one is not forwarded at all, instead of the literal "null".
        setIfPresent(headers, FIRST_NAME, jwt.getClaimAsString("firstName"));
        setIfPresent(headers, LAST_NAME, jwt.getClaimAsString("lastName"));
        setIfPresent(headers, PHONE_NUMBER, jwt.getClaimAsString("phoneNumber"));
    }

    private static void setIfPresent(HttpHeaders headers, String name, String value) {
        if (value != null && !value.isBlank()) {
            headers.set(name, value);
        }
    }
}
