package io.github.himmireda.ridehailing.config.security;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.http.HttpHeaders;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test of the filter alone, with a mock exchange. Paths like "/api/users-admin" have no gateway
 * route today, so this behaviour can't be observed end to end; the unit test pins it down anyway.
 */
class JwtHeaderFilterTest {

    private final JwtHeaderFilter filter = new JwtHeaderFilter();

    @ParameterizedTest
    @ValueSource(strings = {"/api/users", "/api/users/", "/api/users/auth"})
    void usersEndpointsArePublic(String path) {
        assertThat(JwtHeaderFilter.isPublicPath(path)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"/api/users-admin", "/api/usersettings", "/api/user", "/api/rides", "/"})
    void lookalikePathsAreNotPublic(String path) {
        assertThat(JwtHeaderFilter.isPublicPath(path)).isFalse();
    }

    @Test
    void lookalikePathGetsIdentityHeadersFromTheToken() {
        HttpHeaders forwarded = filterAndCaptureHeaders("/api/users-admin");

        assertThat(forwarded.get("X-User-Id")).containsExactly("user-42");
        assertThat(forwarded.get("X-Role")).containsExactly("USER");
    }

    @Test
    void publicPathForwardsNoIdentityHeaders() {
        HttpHeaders forwarded = filterAndCaptureHeaders("/api/users/auth");

        assertThat(forwarded.containsKey("X-User-Id")).isFalse();
        assertThat(forwarded.containsKey("X-Role")).isFalse();
    }

    /** Runs the filter for an authenticated request that also carries a spoofed X-User-Id. */
    private HttpHeaders filterAndCaptureHeaders(String path) {
        Jwt jwt = Jwt.withTokenValue("token")
                .header("alg", "HS512")
                .subject("user-42")
                .claim("role", "USER")
                .claim("email", "rider@example.com")
                .issuedAt(Instant.now())
                .expiresAt(Instant.now().plusSeconds(60))
                .build();
        ServerWebExchange exchange = MockServerWebExchange
                .from(MockServerHttpRequest.get(path).header("X-User-Id", "spoofed"))
                .mutate()
                .principal(Mono.just(new JwtAuthenticationToken(jwt)))
                .build();

        AtomicReference<ServerWebExchange> passedOn = new AtomicReference<>();
        GatewayFilterChain chain = next -> {
            passedOn.set(next);
            return Mono.empty();
        };

        StepVerifier.create(filter.filter(exchange, chain)).verifyComplete();
        return passedOn.get().getRequest().getHeaders();
    }
}
