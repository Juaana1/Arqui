package Arqui3.Entrega3.repository;

import Arqui3.Entrega3.dto.CarreraDTO;
import Arqui3.Entrega3.dto.EstudianteCarreraDTO;
import Arqui3.Entrega3.dto.EstudianteDTO;
import Arqui3.Entrega3.dto.RegistroCarrerasDTO;
import Arqui3.Entrega3.entity.Carrera;
import Arqui3.Entrega3.entity.Estudiante;
import Arqui3.Entrega3.entity.EstudianteCarrera;
import Arqui3.Entrega3.entity.EstudianteCarreraId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface EstudianteCarreraRepository extends JpaRepository<EstudianteCarrera, EstudianteCarreraId> {

    @Query("SELECT ec FROM EstudianteCarrera ec WHERE ec.estudiante = :estudiante AND ec.carrera = :carrera")
    EstudianteCarrera getEstudianteCarrera(@Param("estudiante") Estudiante estudiante, @Param("carrera") Carrera carrera);

    @Query("SELECT new Arqui3.Entrega3.dto.EstudianteCarreraDTO(ec.estudiante.dni, ec.carrera.id,ec.antiguedad,ec.esGraduado, ec.fechaInscripcion, ec.fechaEgreso) FROM EstudianteCarrera ec")
    List<EstudianteCarreraDTO> getEstudiantesCarreras();

    @Modifying
    @Query("UPDATE EstudianteCarrera ec SET ec.estudiante = :estudiante, ec.carrera = :carrera, ec.antiguedad = :antiguedad, ec.esGraduado = :esGraduado, ec.fechaInscripcion = :fechaInscripcion, ec.fechaEgreso = :fechaEgreso WHERE ec.carrera.id = :idCarrera AND ec.estudiante.dni = :dniEstudiante")
    void updateEstudianteCarrera(@Param("estudiante") Estudiante estudiante, @Param("carrera") Carrera carrera,
                                 @Param("antiguedad") int antiguedad, @Param("esGraduado") boolean esGraduado,
                                 @Param("fechaInscripcion") int fechaInscripcion, @Param("fechaEgreso") int fechaEgreso,
                                 @Param("dniEstudiante") int dniEstudiante, @Param("idCarrera") Long idCarrera);

    @Query("DELETE FROM EstudianteCarrera ec WHERE ec.estudiante = :estudiante AND ec.carrera = :carrera")
    void deleteCarrera(@Param("estudiante") Estudiante estudiante, @Param("carrera") Carrera carrera);

    @Query("SELECT new Arqui3.Entrega3.dto.EstudianteDTO(ec.estudiante.dni, ec.estudiante.nombre, ec.estudiante.apellido, ec.estudiante.legajo, ec.estudiante.genero ) " +
            "FROM EstudianteCarrera ec " +
            "WHERE ec.carrera.id = :id AND ec.estudiante.ciudad = :ciudad")
    List<EstudianteDTO> getEstudiantesByCarreraFiltroCiudad(@Param("id") long id, @Param("ciudad") String ciudad);

    @Query("SELECT new Arqui3.Entrega3.dto.CarreraDTO(c.Nombre, COUNT(ec)) FROM EstudianteCarrera ec JOIN Carrera c ON ec.carrera.id = c.id GROUP BY (ec.carrera.Nombre)ORDER BY COUNT(ec) DESC")
    List<CarreraDTO> getCarrerasInscriptosOrdenadas();

    @Query("SELECT new Arqui3.Entrega3.dto.RegistroCarrerasDTO(c.Nombre, ec.fechaEgreso, null, COUNT(e.dni)) " +
            "FROM EstudianteCarrera ec " +
            "JOIN ec.carrera c " + // Usa ec.carrera en lugar de ec.carrera.id
            "JOIN ec.estudiante e " + // Usa ec.estudiante en lugar de e.dni
            "GROUP BY c.Nombre, ec.fechaEgreso " +
            "ORDER BY c.Nombre, ec.fechaEgreso")
    List<RegistroCarrerasDTO> getEgresadosByYear();

    @Query("SELECT new Arqui3.Entrega3.dto.RegistroCarrerasDTO(c.Nombre, ec.fechaInscripcion, COUNT(e.dni), null) " +
            "FROM EstudianteCarrera ec " +
            "JOIN ec.carrera c " + // Usa ec.carrera en lugar de ec.carrera.id
            "JOIN ec.estudiante e " + // Usa ec.estudiante en lugar de e.dni
            "GROUP BY c.Nombre, ec.fechaInscripcion " +
            "ORDER BY c.Nombre, ec.fechaInscripcion")
    List<RegistroCarrerasDTO> getInscriptosByYear();



  /*  @Query("")
    RegistroCarrerasDTO getReporteCarreras();
*/

}

