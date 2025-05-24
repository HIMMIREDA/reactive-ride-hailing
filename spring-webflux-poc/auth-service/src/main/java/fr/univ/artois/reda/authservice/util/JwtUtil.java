package fr.univ.artois.reda.authservice.util;

import fr.univ.artois.reda.authservice.enums.Role;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.util.Date;

@Component
public class JwtUtil {

    @Value("${jwt.secret}")
    private String secret;

    private SecretKey secretKey;


    @PostConstruct
    private void init() {
        this.secretKey = Keys.hmacShaKeyFor(secret.getBytes(StandardCharsets.UTF_8));
    }

    public Mono<String> generateToken(String username, String userId, Role role) {
        return Mono.fromCallable(() -> Jwts.builder()
                        .setSubject(userId)
                        .claim("role", role.toString())
                        .claim("username", username)
                        .setIssuedAt(new Date())
                        .signWith(secretKey)
                        .setExpiration(new Date(System.currentTimeMillis() + 86_400_000L)) // 1 day
                        .compact()
                )
                .log()
                .subscribeOn(Schedulers.boundedElastic());
    }


}
