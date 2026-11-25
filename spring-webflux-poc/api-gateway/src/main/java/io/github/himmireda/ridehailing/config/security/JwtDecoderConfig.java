package io.github.himmireda.ridehailing.config.security;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtClaimNames;
import org.springframework.security.oauth2.jwt.JwtClaimValidator;
import org.springframework.security.oauth2.jwt.JwtValidators;
import org.springframework.security.oauth2.jwt.NimbusReactiveJwtDecoder;
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoder;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.util.ArrayList;
import java.util.List;

@Configuration
public class JwtDecoderConfig {

    /**
     * Claims without which a request cannot be attributed or authorized: a token missing one of them
     * is rejected (401). Profile claims (firstName, lastName, phoneNumber) are optional, because
     * sign-up does not require them; JwtHeaderFilter simply does not forward a missing one.
     */
    static final List<String> REQUIRED_CLAIMS = List.of(JwtClaimNames.SUB, "role", "email");

    @Value("${jwt.secret}")
    private String secret;

    @Bean
    public ReactiveJwtDecoder jwtDecoder() {
        SecretKey secretKey = new SecretKeySpec(JwtSecretValidator.requireValidSecret(secret), "HmacSHA512");

        NimbusReactiveJwtDecoder decoder = NimbusReactiveJwtDecoder.withSecretKey(secretKey)
                .macAlgorithm(MacAlgorithm.HS512)
                .build();

        List<OAuth2TokenValidator<Jwt>> validators = new ArrayList<>();
        validators.add(JwtValidators.createDefault()); // expiry / not-before, as before
        REQUIRED_CLAIMS.forEach(claim -> validators.add(new JwtClaimValidator<Object>(claim,
                value -> value instanceof String text && !text.isBlank())));
        decoder.setJwtValidator(new DelegatingOAuth2TokenValidator<>(validators));
        return decoder;
    }
}
