package Arqui3.Entrega3.repository;

import Arqui3.Entrega3.dto.EstudianteDTO;
import Arqui3.Entrega3.entity.Estudiante;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface EstudianteRepository extends JpaRepository<Estudiante, Integer> {

    @Query("SELECT e FROM Estudiante e WHERE e.dni = :dni")
    public Estudiante getEstudianteByDNI(int dni);

    @Query("SELECT new Arqui3.Entrega3.dto.EstudianteDTO(e.dni, e.nombre, e.apellido, e.legajo) FROM Estudiante e ORDER BY e.apellido")
    List<EstudianteDTO> getEstudiantesOrdenadosByApellido();

    @Modifying
    @Query("UPDATE Estudiante e SET e.nombre = :nombre, e.apellido = :apellido, e.legajo = :legajo WHERE e.dni = :dni")
    void updateEstudiante(@Param("nombre") String nombre, @Param("apellido")String apellido, @Param("legajo")int legajo, @Param("dni") int dni);

    @Query("DELETE FROM Estudiante e WHERE e.dni = :dni")
    void deleteEstudiante(int dni);

    @Query("SELECT new Arqui3.Entrega3.dto.EstudianteDTO(e.dni, e.nombre, e.apellido, e.legajo) FROM Estudiante e WHERE e.legajo = :legajo")
    EstudianteDTO getEstudianteByLegajo(@Param("legajo") int legajo);

    @Query("SELECT new Arqui3.Entrega3.dto.EstudianteDTO(e.dni, e.nombre, e.apellido, e.legajo, e.genero) FROM Estudiante e WHERE e.genero = :genero")
    List<EstudianteDTO>getEstudiantesByGenero(@Param("genero") String genero);

    @Query("SELECT e FROM Estudiante e WHERE e.dni = :dni")
    Estudiante getEstudianteByDni(int dni);
}
