package repository;

import dto.CarreraDTO;
import dto.EstudianteDTO;
import dto.RegistroCarrerasDTO;
import entity.Carrera;
import entity.Estudiante;
import entity.EstudianteCarrera;

import java.util.List;

public interface EstudianteCarreraRepository {
    EstudianteCarrera getEstudianteCarrera(Estudiante estudiante, Carrera carrera);
    List<EstudianteCarrera> getEstudiantesCarreras();
    void addEstudianteCarrera(EstudianteCarrera estudianteCarrera);
    void updateCarrera(EstudianteCarrera estudianteCarrera);
    void deleteCarrera(Estudiante estudiante, Carrera carrera);
    List<EstudianteDTO>getEstudiantesByCarreraFiltroCiudad(String carrera, String ciudad);
    List<CarreraDTO>getCarrerasInscriptosOrdenadas();
    List<RegistroCarrerasDTO>reporteCarreras();
}
