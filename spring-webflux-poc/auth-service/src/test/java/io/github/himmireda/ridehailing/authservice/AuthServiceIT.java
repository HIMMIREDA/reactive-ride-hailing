package io.github.himmireda.ridehailing.authservice;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Full application on a random port, against a real PostgreSQL started by Testcontainers.
 *
 * <ul>
 *   <li>{@code @Testcontainers} + {@code @Container}: JUnit starts the container before the tests
 *       (a static field means once for the whole class) and stops it afterwards.</li>
 *   <li>{@code @ServiceConnection}: Spring Boot reads the container's host, port and credentials and
 *       publishes them as "connection details" that its auto-configuration uses (here, the R2DBC
 *       ConnectionFactory). No URL to copy around.</li>
 *   <li>{@code @DynamicPropertySource}: sets plain properties from the container. Needed here because
 *       the custom FlywayConfig reads {@code spring.flyway.url} and the R2DBC username/password
 *       properties directly, which {@code @ServiceConnection} does not set.</li>
 * </ul>
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class AuthServiceIT {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:17-alpine");

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.flyway.url", postgres::getJdbcUrl);
        registry.add("spring.r2dbc.username", postgres::getUsername);
        registry.add("spring.r2dbc.password", postgres::getPassword);
    }

    @Autowired
    private WebTestClient webTestClient;

    @Value("${jwt.secret}")
    private String jwtSecret;

    @Test
    void signUpThenLoginReturnsSignedJwt() {
        // A unique email per run: the test does not depend on what other tests left in the database.
        String email = "rider-" + UUID.randomUUID() + "@example.com";

        Map<String, Object> created = webTestClient.post().uri("/api/users")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of(
                        "email", email, "password", "pass1234",
                        "firstName", "Ada", "lastName", "Lovelace",
                        "role", "USER", "phoneNumber", "0600000001"))
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.CREATED)
                .expectBody(new org.springframework.core.ParameterizedTypeReference<Map<String, Object>>() {})
                .returnResult().getResponseBody();
        assertThat(created).containsEntry("email", email).containsEntry("role", "USER");
        assertThat(created).doesNotContainKey("password");
        String userId = (String) created.get("id");

        Map<String, Object> login = webTestClient.post().uri("/api/users/auth")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("email", email, "password", "pass1234", "role", "USER"))
                .exchange()
                .expectStatus().isOk()
                .expectBody(new org.springframework.core.ParameterizedTypeReference<Map<String, Object>>() {})
                .returnResult().getResponseBody();
        String token = (String) login.get("token");
        assertThat(token).isNotBlank();

        // The token must verify with the service's secret and carry the user's identity.
        Claims claims = Jwts.parserBuilder()
                .setSigningKey(Keys.hmacShaKeyFor(jwtSecret.getBytes(StandardCharsets.UTF_8)))
                .build()
                .parseClaimsJws(token)
                .getBody();
        assertThat(claims.getSubject()).isEqualTo(userId);
        assertThat(claims.get("email", String.class)).isEqualTo(email);
        assertThat(claims.get("role", String.class)).isEqualTo("USER");
        assertThat(claims.getExpiration()).isInTheFuture();
    }

    @Test
    void loginWithWrongPasswordIsRejected() {
        String email = "driver-" + UUID.randomUUID() + "@example.com";
        webTestClient.post().uri("/api/users")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of(
                        "email", email, "password", "right-password",
                        "firstName", "Grace", "lastName", "Hopper",
                        "role", "DRIVER", "phoneNumber", "0600000002"))
                .exchange()
                .expectStatus().isCreated();

        webTestClient.post().uri("/api/users/auth")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("email", email, "password", "wrong-password", "role", "DRIVER"))
                .exchange()
                .expectStatus().isUnauthorized();
    }
}
