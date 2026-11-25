package io.github.himmireda.ridehailing.authservice.dto.response;

import io.github.himmireda.ridehailing.authservice.enums.Role;
import lombok.*;

import java.util.UUID;

@AllArgsConstructor
@NoArgsConstructor
@Getter  @Setter
@Builder
public class AuthResponseDto
{
        private UUID id;
        private String email;
        private String firstName;
        private String lastName;
        private String phoneNumber;
        private String token;
        private Role role;
}
