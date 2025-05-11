package fr.univ.artois.reda.userservice.mapper;

import fr.univ.artois.reda.userservice.dto.request.UserRegisterRequestDto;
import fr.univ.artois.reda.userservice.dto.response.UserRegisterResponseDto;
import fr.univ.artois.reda.userservice.entity.UserEntity;
import javax.annotation.processing.Generated;
import org.springframework.stereotype.Component;

@Generated(
    value = "org.mapstruct.ap.MappingProcessor",
    date = "2025-05-11T23:09:03+0200",
    comments = "version: 1.6.3, compiler: javac, environment: Java 21.0.2 (Amazon.com Inc.)"
)
@Component
public class UserMapperImpl implements UserMapper {

    @Override
    public UserRegisterResponseDto mapToUserRegisterResponseDto(UserEntity userEntity) {
        if ( userEntity == null ) {
            return null;
        }

        String id = null;
        String email = null;
        String firstName = null;
        String lastName = null;

        if ( userEntity.getId() != null ) {
            id = String.valueOf( userEntity.getId() );
        }
        email = userEntity.getEmail();
        firstName = userEntity.getFirstName();
        lastName = userEntity.getLastName();

        UserRegisterResponseDto userRegisterResponseDto = new UserRegisterResponseDto( id, email, firstName, lastName );

        return userRegisterResponseDto;
    }

    @Override
    public UserEntity mapToUserEntity(UserRegisterRequestDto userRegisterRequestDto) {
        if ( userRegisterRequestDto == null ) {
            return null;
        }

        UserEntity.UserEntityBuilder userEntity = UserEntity.builder();

        userEntity.firstName( userRegisterRequestDto.firstName() );
        userEntity.lastName( userRegisterRequestDto.lastName() );
        userEntity.email( userRegisterRequestDto.email() );
        userEntity.password( userRegisterRequestDto.password() );

        return userEntity.build();
    }
}
