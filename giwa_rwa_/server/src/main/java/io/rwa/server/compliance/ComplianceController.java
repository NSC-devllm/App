package io.rwa.server.compliance;

import io.rwa.server.wallet.ComplianceStatus;
import io.rwa.server.wallet.UserEntity;
import jakarta.validation.Valid;
import java.util.List;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/admin/compliance")
public class ComplianceController {

    private final ComplianceService complianceService;

    public ComplianceController(ComplianceService complianceService) {
        this.complianceService = complianceService;
    }

    @PostMapping("/approve")
    public Map<String, Object> approve(@Valid @RequestBody ComplianceRequest request) {
        UserEntity user = complianceService.approve(request.userId());
        return Map.of("userId", user.getUserId(), "status", user.getComplianceStatus().name());
    }

    @PostMapping("/revoke")
    public Map<String, Object> revoke(@Valid @RequestBody ComplianceRequest request) {
        UserEntity user = complianceService.revoke(request.userId());
        return Map.of("userId", user.getUserId(), "status", user.getComplianceStatus().name());
    }

    @GetMapping("/users")
    public List<Map<String, Object>> users(@RequestParam("status") String status) {
        return complianceService.findByStatus(ComplianceStatus.valueOf(status.toUpperCase())).stream()
            .map(user -> Map.<String, Object>of(
                "userId", user.getUserId(),
                "status", user.getComplianceStatus().name(),
                "updatedAt", user.getComplianceUpdatedAt()
            ))
            .toList();
    }
}
