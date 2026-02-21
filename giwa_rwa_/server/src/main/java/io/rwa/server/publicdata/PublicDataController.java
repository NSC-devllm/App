package io.rwa.server.publicdata;

import jakarta.validation.Valid;
import java.util.List;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping
public class PublicDataController {

    private final PublicDataService publicDataService;

    public PublicDataController(PublicDataService publicDataService) {
        this.publicDataService = publicDataService;
    }

    @PostMapping("/admin/complexes/import")
    public Map<String, Object> importComplex(@Valid @RequestBody ComplexImportRequest request) {
        return publicDataService.importComplex(request.rawItemJson());
    }

    @GetMapping("/complexes")
    public List<ComplexEntity> listComplexes() {
        return publicDataService.listComplexes();
    }

    @GetMapping("/complexes/{kaptCode}")
    public ComplexEntity getComplex(@PathVariable String kaptCode) {
        return publicDataService.getComplex(kaptCode);
    }

    @GetMapping("/complexes/{kaptCode}/classes")
    public List<PropertyClassEntity> classes(@PathVariable String kaptCode) {
        return publicDataService.getClasses(kaptCode);
    }

    @GetMapping("/classes/{classId}/units")
    public List<UnitEntity> units(@PathVariable String classId) {
        return publicDataService.getUnits(classId);
    }
}
