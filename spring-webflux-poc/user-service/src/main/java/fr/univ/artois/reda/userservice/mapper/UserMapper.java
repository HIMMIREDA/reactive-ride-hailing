package fr.univ.artois.reda.userservice.mapper;

import fr.univ.artois.reda.userservice.dto.request.UserRegisterRequestDto;
import fr.univ.artois.reda.userservice.dto.response.UserRegisterResponseDto;
import fr.univ.artois.reda.userservice.entity.UserEntity;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

@Mapper(componentModel = "spring")
public interface UserMapper {

    UserRegisterResponseDto mapToUserRegisterResponseDto(UserEntity userEntity);
    UserEntity mapToUserEntity(UserRegisterRequestDto userRegisterRequestDto);
}
