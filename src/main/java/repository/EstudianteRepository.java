package repository;

import dto.EstudianteDTO;
import entity.Estudiante;

import java.util.List;

public interface EstudianteRepository {

    Estudiante getEstudiante(Estudiante estudiante);
    List<EstudianteDTO> getEstudiantesOrdenadosByApellido();
    void addEstudiante(Estudiante estudiante);
    void updateEstudiante(Estudiante estudiante);
    void deleteEstudiante(Estudiante estudiante);
    EstudianteDTO getEstudianteByLegajo(int legajo);
    List<EstudianteDTO>getEstudiantesByGenero(String genero);
}
