package Arqui3.Entrega3.repository;

import Arqui3.Entrega3.dto.EstudianteDTO;
import Arqui3.Entrega3.dto.RegistroCarrerasDTO;
import Arqui3.Entrega3.entity.Carrera;
import org.springframework.data.jpa.repository.*;
import Arqui3.Entrega3.dto.CarreraDTO;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface CarreraRepository extends JpaRepository<Carrera, Integer> {

    @Query("SELECT c FROM Carrera c WHERE c.id = :id")
    Carrera getCarrera(@Param("id") Long id);

    @Query("SELECT new Arqui3.Entrega3.dto.CarreraDTO(c.Nombre) FROM Carrera c")
    List<CarreraDTO> getCarreras();

    @Modifying
    @Query("UPDATE Carrera c SET c.Nombre = :nombre WHERE c.id = :id")
    void updateCarrera(@Param("nombre") String nombre, @Param("id") Long id);

    @Query("DELETE FROM Carrera c WHERE c.id = :id")
    void deleteCarrera(@Param("id") Long id);


}
