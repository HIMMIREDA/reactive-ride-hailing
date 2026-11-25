package io.github.himmireda.ridehailing.authservice.mapper;

import io.github.himmireda.ridehailing.authservice.dto.request.UserRegisterRequestDto;
import io.github.himmireda.ridehailing.authservice.dto.response.AuthResponseDto;
import io.github.himmireda.ridehailing.authservice.dto.response.UserRegisterResponseDto;
import io.github.himmireda.ridehailing.authservice.entity.UserEntity;
import org.mapstruct.Mapper;

@Mapper(componentModel = "spring")
public interface UserMapper {

    UserRegisterResponseDto mapToUserRegisterResponseDto(UserEntity userEntity);
    UserEntity mapToUserEntity(UserRegisterRequestDto userRegisterRequestDto);
    AuthResponseDto mapToAuthResponseDto(UserEntity userEntity);
}
