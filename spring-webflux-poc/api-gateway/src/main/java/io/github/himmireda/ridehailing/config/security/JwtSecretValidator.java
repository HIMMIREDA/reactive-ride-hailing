package io.github.himmireda.ridehailing.config.security;

import java.nio.charset.StandardCharsets;

/**
 * Validates the shared HS512 secret at startup, so a missing or weak JWT_SECRET stops the service
 * immediately with an actionable message instead of failing later on the first login or request.
 */
public final class JwtSecretValidator {

    /** HS512 needs a key of at least 512 bits. A shorter key would also make jjwt pick HS256/HS384. */
    static final int MIN_SECRET_BYTES = 64;

    private static final String HOW_TO_FIX =
            "Set JWT_SECRET in spring-webflux-poc/.env (see .env.example) or in the environment, e.g. "
                    + "JWT_SECRET=$(openssl rand -base64 64 | tr -d '\\n'). "
                    + "auth-service and api-gateway must use the same value.";

    private JwtSecretValidator() {
    }

    public static byte[] requireValidSecret(String secret) {
        if (secret == null || secret.isBlank()) {
            throw new IllegalStateException("JWT_SECRET is not set. " + HOW_TO_FIX);
        }
        byte[] bytes = secret.getBytes(StandardCharsets.UTF_8);
        if (bytes.length < MIN_SECRET_BYTES) {
            throw new IllegalStateException("JWT_SECRET is too short: " + bytes.length + " bytes, HS512 needs at least "
                    + MIN_SECRET_BYTES + ". " + HOW_TO_FIX);
        }
        return bytes;
    }
}
