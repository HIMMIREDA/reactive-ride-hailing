package fr.univ.artois.reda.authservice.dto.request;

import fr.univ.artois.reda.authservice.enums.Role;
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
