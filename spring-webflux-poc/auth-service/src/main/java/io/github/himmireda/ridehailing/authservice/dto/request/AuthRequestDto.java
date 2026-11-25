package io.github.himmireda.ridehailing.authservice.dto.request;

import io.github.himmireda.ridehailing.authservice.enums.Role;
import lombok.*;

@AllArgsConstructor
@NoArgsConstructor
@Getter @Setter
@Builder
public class AuthRequestDto{
        private String email;
        private String password;
        private Role role;
}
