package fr.univ.artois.reda.authservice.mapper;

import fr.univ.artois.reda.authservice.dto.request.UserRegisterRequestDto;
import fr.univ.artois.reda.authservice.dto.response.AuthResponseDto;
import fr.univ.artois.reda.authservice.dto.response.UserRegisterResponseDto;
import fr.univ.artois.reda.authservice.entity.UserEntity;
import fr.univ.artois.reda.authservice.enums.Role;
import java.util.UUID;
import javax.annotation.processing.Generated;
import org.springframework.stereotype.Component;

@Generated(
    value = "org.mapstruct.ap.MappingProcessor",
    date = "2025-05-21T23:39:58+0200",
    comments = "version: 1.6.3, compiler: javac, environment: Java 21.0.2 (Amazon.com Inc.)"
)
@Component
public class UserMapperImpl implements UserMapper {

    @Override
    public UserRegisterResponseDto mapToUserRegisterResponseDto(UserEntity userEntity) {
        if ( userEntity == null ) {
            return null;
        }

        UUID id = null;
        String email = null;
        String firstName = null;
        String lastName = null;
        Role role = null;

        id = userEntity.getId();
        email = userEntity.getEmail();
        firstName = userEntity.getFirstName();
        lastName = userEntity.getLastName();
        role = userEntity.getRole();

        UserRegisterResponseDto userRegisterResponseDto = new UserRegisterResponseDto( id, email, firstName, lastName, role );

        return userRegisterResponseDto;
    }

    @Override
    public UserEntity mapToUserEntity(UserRegisterRequestDto userRegisterRequestDto) {
        if ( userRegisterRequestDto == null ) {
            return null;
        }

        UserEntity.UserEntityBuilder userEntity = UserEntity.builder();

        userEntity.firstName( userRegisterRequestDto.getFirstName() );
        userEntity.lastName( userRegisterRequestDto.getLastName() );
        userEntity.email( userRegisterRequestDto.getEmail() );
        userEntity.password( userRegisterRequestDto.getPassword() );
        userEntity.role( userRegisterRequestDto.getRole() );

        return userEntity.build();
    }

    @Override
    public AuthResponseDto mapToAuthResponseDto(UserEntity userEntity) {
        if ( userEntity == null ) {
            return null;
        }

        AuthResponseDto.AuthResponseDtoBuilder authResponseDto = AuthResponseDto.builder();

        authResponseDto.id( userEntity.getId() );
        authResponseDto.email( userEntity.getEmail() );
        authResponseDto.firstName( userEntity.getFirstName() );
        authResponseDto.lastName( userEntity.getLastName() );
        authResponseDto.role( userEntity.getRole() );

        return authResponseDto.build();
    }
}
