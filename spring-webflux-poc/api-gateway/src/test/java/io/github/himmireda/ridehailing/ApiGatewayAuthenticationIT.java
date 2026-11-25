package io.github.himmireda.ridehailing;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.anyRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The real gateway (Spring Security + Spring Cloud Gateway) in front of a WireMock stub that stands in
 * for auth-, ride- and driver-service. The stub records every request it receives, so the tests can
 * check both what the client gets back and what actually reached the downstream service.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ApiGatewayAuthenticationIT {

    private static final String SECRET = "test-only-secret-for-api-gateway-integration-tests-0123456789abcdef";

    static final WireMockServer downstream = new WireMockServer(options().dynamicPort());

    static {
        downstream.start();
        downstream.stubFor(com.github.tomakehurst.wiremock.client.WireMock.any(anyUrl()).willReturn(ok("from downstream")));
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("JWT_SECRET", () -> SECRET);
        // Point the three routes at the stub. Overriding routes[i].uri would not work: Spring Boot binds a
        // list from one property source only, so it would replace the routes defined in application.yaml.
        registry.add("AUTH_SERVICE_URL", downstream::baseUrl);
        registry.add("RIDE_SERVICE_URL", downstream::baseUrl);
        registry.add("DRIVER_SERVICE_URL", downstream::baseUrl);
    }

    @AfterAll
    static void stopDownstream() {
        downstream.stop();
    }

    @Autowired
    private WebTestClient webTestClient;

    private final String userId = UUID.randomUUID().toString();

    @BeforeEach
    void forgetPreviousRequests() {
        // Each test only looks at the requests it caused, whatever ran before it.
        downstream.resetRequests();
    }

    @Test
    void requestWithoutTokenIsRejectedAndNotForwarded() {
        webTestClient.get().uri("/api/rides/{id}/proposals/stream", UUID.randomUUID())
                .exchange()
                .expectStatus().isUnauthorized()
                .expectHeader().valueMatches(HttpHeaders.WWW_AUTHENTICATE, "Bearer.*");

        assertThat(forwardedRequests()).isEmpty();
    }

    @Test
    void malformedTokenIsRejected() {
        assertUnauthorizedWith("not-a-jwt");
    }

    @Test
    void tokenSignedWithAnotherSecretIsRejected() {
        assertUnauthorizedWith(token("another-secret-that-is-also-long-enough-for-hs512-0123456789abcdef",
                Instant.now().plus(Duration.ofHours(1))));
    }

    @Test
    void expiredTokenIsRejected() {
        // Past the 60 s clock skew Spring Security tolerates by default.
        assertUnauthorizedWith(token(SECRET, Instant.now().minus(Duration.ofMinutes(10))));
    }

    @Test
    void validTokenIsForwardedWithIdentityHeaders() {
        webTestClient.get().uri("/api/rides/{id}/proposals/stream", UUID.randomUUID())
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token(SECRET, Instant.now().plus(Duration.ofHours(1))))
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class).isEqualTo("from downstream");

        LoggedRequest forwarded = singleForwardedRequest();
        assertThat(forwarded.header("X-User-Id").values()).containsExactly(userId);
        assertThat(forwarded.header("X-Role").values()).containsExactly("USER");
        assertThat(forwarded.header("X-Email").values()).containsExactly("rider@example.com");
        assertThat(forwarded.header("X-First-Name").values()).containsExactly("Ada");
        assertThat(forwarded.header("X-Last-Name").values()).containsExactly("Lovelace");
        assertThat(forwarded.header("X-Phone-Number").values()).containsExactly("0600000001");
    }

    @Test
    void clientSuppliedIdentityHeadersAreReplacedByTokenValues() {
        webTestClient.put().uri("/api/rides/{id}/accept", UUID.randomUUID())
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token(SECRET, Instant.now().plus(Duration.ofHours(1))))
                // An authenticated user trying to act as someone else.
                .header("X-User-Id", "victim-user-id")
                .header("x-role", "DRIVER")
                .header("X-Email", "victim@example.com")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{}")
                .exchange()
                .expectStatus().isOk();

        LoggedRequest forwarded = singleForwardedRequest();
        assertThat(forwarded.header("X-User-Id").values()).containsExactly(userId);
        assertThat(forwarded.header("X-Role").values()).containsExactly("USER");
        assertThat(forwarded.header("X-Email").values()).containsExactly("rider@example.com");
    }

    @Test
    void identityHeadersAreStrippedOnPublicRoutes() {
        webTestClient.post().uri("/api/users/auth")
                .header("X-User-Id", "victim-user-id")
                .header("X-Role", "DRIVER")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{}")
                .exchange()
                .expectStatus().isOk();

        LoggedRequest forwarded = singleForwardedRequest();
        assertThat(forwarded.containsHeader("X-User-Id")).isFalse();
        assertThat(forwarded.containsHeader("X-Role")).isFalse();
    }

    private void assertUnauthorizedWith(String token) {
        webTestClient.get().uri("/api/rides/{id}/proposals/stream", UUID.randomUUID())
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                .exchange()
                .expectStatus().isUnauthorized();

        assertThat(forwardedRequests()).isEmpty();
    }

    private List<LoggedRequest> forwardedRequests() {
        return downstream.findAll(anyRequestedFor(anyUrl()));
    }

    private LoggedRequest singleForwardedRequest() {
        List<LoggedRequest> requests = forwardedRequests();
        assertThat(requests).hasSize(1);
        return requests.get(0);
    }

    /** Same shape as the tokens auth-service issues: HS512, subject = user id, profile claims. */
    private String token(String secret, Instant expiresAt) {
        try {
            JWTClaimsSet claims = new JWTClaimsSet.Builder()
                    .subject(userId)
                    .claim("role", "USER")
                    .claim("email", "rider@example.com")
                    .claim("firstName", "Ada")
                    .claim("lastName", "Lovelace")
                    .claim("phoneNumber", "0600000001")
                    .issueTime(Date.from(expiresAt.minus(Duration.ofDays(1))))
                    .expirationTime(Date.from(expiresAt))
                    .build();
            SignedJWT jwt = new SignedJWT(new JWSHeader(JWSAlgorithm.HS512), claims);
            jwt.sign(new MACSigner(secret.getBytes(StandardCharsets.UTF_8)));
            return jwt.serialize();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
