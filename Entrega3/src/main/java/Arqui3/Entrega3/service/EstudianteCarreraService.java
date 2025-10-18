package Arqui3.Entrega3.service;

import Arqui3.Entrega3.dto.CarreraDTO;
import Arqui3.Entrega3.dto.EstudianteCarreraDTO;
import Arqui3.Entrega3.dto.EstudianteDTO;
import Arqui3.Entrega3.dto.RegistroCarrerasDTO;
import Arqui3.Entrega3.entity.Carrera;
import Arqui3.Entrega3.entity.Estudiante;
import Arqui3.Entrega3.entity.EstudianteCarrera;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import Arqui3.Entrega3.repository.EstudianteCarreraRepository;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

@Service
public class EstudianteCarreraService {

    @Autowired
    private EstudianteCarreraRepository estudianteCarreraRepository;

    public EstudianteCarrera getEstudianteCarrera(Estudiante estudiante, Carrera carrera){
        return estudianteCarreraRepository.getEstudianteCarrera(estudiante, carrera);
    }

    public List<EstudianteCarreraDTO> getEstudiantesCarreras(){
        return estudianteCarreraRepository.getEstudiantesCarreras();
    }

    public void updateCarrera(Estudiante estudiante, Carrera carrera, int antiguedad, boolean esGraduado, int fechaInscripcion, int fechaEgreso, int idEstudiante, Long idCarrera){
        estudianteCarreraRepository.updateEstudianteCarrera(estudiante, carrera, antiguedad, esGraduado, fechaInscripcion, fechaEgreso, idEstudiante, idCarrera);
    }

    public void deleteCarrera(Estudiante estudiante, Carrera carrera){
        estudianteCarreraRepository.deleteCarrera(estudiante, carrera);
    }

    public List<EstudianteDTO>getEstudiantesByCarreraFiltroCiudad(long id, String ciudad){
        return estudianteCarreraRepository.getEstudiantesByCarreraFiltroCiudad(id, ciudad);
    }

    public List<CarreraDTO>getCarrerasInscriptosOrdenadas(){
        return estudianteCarreraRepository.getCarrerasInscriptosOrdenadas();
    }

    public EstudianteCarrera addEstudianteCarrera(EstudianteCarrera estudianteCarrera){
        return estudianteCarreraRepository.save(estudianteCarrera);
    }

    public List<RegistroCarrerasDTO> reporteCarreras() {
        List<RegistroCarrerasDTO> reporteaInscriptos = this.estudianteCarreraRepository.getInscriptosByYear();
        List<RegistroCarrerasDTO> reporteaEgresados = this.estudianteCarreraRepository.getEgresadosByYear();

        // Crear una lista para el reporte de incriptos
        List<RegistroCarrerasDTO> reporte = new ArrayList<>(reporteaInscriptos);

        // Usar un iterador para evitar ConcurrentModificationException
        Iterator<RegistroCarrerasDTO> iterator = reporteaEgresados.iterator();
        while (iterator.hasNext()) {
            RegistroCarrerasDTO egresado = iterator.next();
            for (RegistroCarrerasDTO inscripto : reporteaInscriptos) {
                if (egresado.getNombreCarrera().equals(inscripto.getNombreCarrera()) &&
                        egresado.getAnio() == inscripto.getAnio()) {
                    inscripto.setCantEgresados(egresado.getCantEgresados());
                    iterator.remove(); // Remover el egresado
                    break; // Salir del bucle interno
                }
            }
        }

        // Agregar los egresados que no tienen inscriptos correspondientes
        reporte.addAll(reporteaEgresados);
        // Ordenar el reporte final
        reporte.sort(Comparator.comparing(RegistroCarrerasDTO::getNombreCarrera)
                .thenComparing(RegistroCarrerasDTO::getAnio));

        return reporte;
    }

   /* public RegistroCarrerasDTO getReporteCarreras(){
        return estudianteCarreraRepository.getReporteCarreras();
    }*/

}
