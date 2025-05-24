package fr.univ.artois.reda.authservice.mapper;

import fr.univ.artois.reda.authservice.dto.request.UserRegisterRequestDto;
import fr.univ.artois.reda.authservice.dto.response.AuthResponseDto;
import fr.univ.artois.reda.authservice.dto.response.UserRegisterResponseDto;
import fr.univ.artois.reda.authservice.entity.UserEntity;
import org.mapstruct.Mapper;

@Mapper(componentModel = "spring")
public interface UserMapper {

    UserRegisterResponseDto mapToUserRegisterResponseDto(UserEntity userEntity);
    UserEntity mapToUserEntity(UserRegisterRequestDto userRegisterRequestDto);
    AuthResponseDto mapToAuthResponseDto(UserEntity userEntity);
}
