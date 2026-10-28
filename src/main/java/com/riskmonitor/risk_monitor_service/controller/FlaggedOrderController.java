package com.riskmonitor.risk_monitor_service.controller;

import java.util.List;

import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestParam;

import com.riskmonitor.risk_monitor_service.dto.FlaggedOrderDTO;
import com.riskmonitor.risk_monitor_service.service.FlaggedOrderService;

import lombok.RequiredArgsConstructor;


@RestController
@RequestMapping("/api/Orders")
@CrossOrigin(origins = "http://localhost:3000")
@RequiredArgsConstructor
public class FlaggedOrderController {

    private final FlaggedOrderService flaggedOrderService;

    // @GetMapping
    // public List<FlaggedOrderDTO> getAllFlaggedOrders(
    //     @RequestParam(required = false) String asset,
    //     @RequestParam(required = false) String rule,
    //     @RequestMapping(default = "0") int page,
    //     @RequestMapping(default = "20") int size
    // ){
    //     return flaggedOrderService.getAllFlaggedOrders(asset, rule, page, size);        
    // }

    @GetMapping
    public List<FlaggedOrderDTO> getAllFlaggedOrders(
            @RequestParam(required = false) String asset) {
        return flaggedOrderService.getAllFlaggedOrders(asset);
    }





}
