package io.rwa.server.publicdata;

import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PropertyClassRepository extends JpaRepository<PropertyClassEntity, String> {
    List<PropertyClassEntity> findByKaptCodeOrderByClassKeyAsc(String kaptCode);
    Optional<PropertyClassEntity> findByKaptCodeAndClassKey(String kaptCode, String classKey);
}
