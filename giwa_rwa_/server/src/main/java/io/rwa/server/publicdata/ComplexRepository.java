package io.rwa.server.publicdata;

import org.springframework.data.jpa.repository.JpaRepository;

public interface ComplexRepository extends JpaRepository<ComplexEntity, String> {
}
