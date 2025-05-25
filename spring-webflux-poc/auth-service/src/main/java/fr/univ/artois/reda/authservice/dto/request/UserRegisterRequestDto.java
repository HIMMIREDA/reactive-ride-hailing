package fr.univ.artois.reda.authservice.dto.request;

import fr.univ.artois.reda.authservice.enums.Role;
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
