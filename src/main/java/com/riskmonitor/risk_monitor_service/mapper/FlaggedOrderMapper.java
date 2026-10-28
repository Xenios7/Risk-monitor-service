package com.riskmonitor.risk_monitor_service.mapper;

import java.util.List;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

import com.riskmonitor.risk_monitor_service.dto.FlaggedOrderDTO;
import com.riskmonitor.risk_monitor_service.model.FlaggedOrder;

@Mapper(componentModel = "spring")
public interface FlaggedOrderMapper {

    @Mapping(source = "riskRule.ruleType", target = "ruleType")
    FlaggedOrderDTO toDTO(FlaggedOrder entity);

    List<FlaggedOrderDTO> toDTOList(List<FlaggedOrder> entities);
}