package io.github.himmireda.ridehailing.authservice.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JwtSecretValidatorTest {

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"", "   "})
    void missingSecretFailsWithInstructions(String secret) {
        assertThatThrownBy(() -> JwtSecretValidator.requireValidSecret(secret))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("JWT_SECRET is not set.")
                .hasMessageContaining("openssl rand -base64 64");
    }

    @Test
    void secretShorterThan64BytesIsRejected() {
        assertThatThrownBy(() -> JwtSecretValidator.requireValidSecret("x".repeat(63)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("too short: 63 bytes");
    }

    @Test
    void placeholderFromEnvExampleIsRejected() {
        assertThatThrownBy(() -> JwtSecretValidator.requireValidSecret("replace-me-with-the-output-of-openssl-rand-base64-64"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("too short");
    }

    @Test
    void secretOf64BytesIsAccepted() {
        assertThat(JwtSecretValidator.requireValidSecret("x".repeat(64))).hasSize(64);
    }
}
