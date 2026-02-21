package io.rwa.server.publicdata;

import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface UnitRepository extends JpaRepository<UnitEntity, String> {
    List<UnitEntity> findByClassIdOrderByUnitNoAsc(String classId);
    Optional<UnitEntity> findByClassIdAndUnitNo(String classId, int unitNo);
    Optional<UnitEntity> findByUnitId(String unitId);
}
