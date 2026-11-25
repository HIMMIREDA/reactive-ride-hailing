package io.github.himmireda.ridehailing.authservice.util;

import io.github.himmireda.ridehailing.authservice.entity.UserEntity;
import io.github.himmireda.ridehailing.authservice.enums.Role;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import javax.crypto.SecretKey;
import java.util.Date;

@Component
public class JwtUtil {

    @Value("${jwt.secret}")
    private String secret;

    private SecretKey secretKey;


    @PostConstruct
    private void init() {
        this.secretKey = Keys.hmacShaKeyFor(JwtSecretValidator.requireValidSecret(secret));
    }

    public Mono<String> generateToken(UserEntity user) {
        return Mono.fromCallable(() -> Jwts.builder()
                        .setSubject(user.getId().toString())
                        .claim("role", user.getRole().toString())
                        .claim("email", user.getEmail())
                        .claim("firstName", user.getFirstName())
                        .claim("lastName", user.getLastName())
                        .claim("phoneNumber", user.getPhoneNumber())
                        .setIssuedAt(new Date())
                        .signWith(secretKey)
                        .setExpiration(new Date(System.currentTimeMillis() + 86_400_000L)) // 1 day
                        .compact()
                )
                .log()
                .subscribeOn(Schedulers.boundedElastic());
    }


}
