package io.github.himmireda.ridehailing.authservice.dto.request;

import io.github.himmireda.ridehailing.authservice.enums.Role;
import lombok.*;

@AllArgsConstructor
@NoArgsConstructor
@Getter @Setter
@Builder
public class UserRegisterRequestDto{
        private String email;
        private String password;
        private String firstName;
        private String lastName;
        private Role role;
        private String phoneNumber;

}
