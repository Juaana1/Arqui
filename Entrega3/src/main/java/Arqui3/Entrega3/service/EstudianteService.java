package Arqui3.Entrega3.service;

import Arqui3.Entrega3.dto.EstudianteDTO;
import Arqui3.Entrega3.entity.Estudiante;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import Arqui3.Entrega3.repository.EstudianteRepository;

import java.util.List;

@Service
public class EstudianteService {

    @Autowired
    private EstudianteRepository estudianteRepository;

    public EstudianteDTO getEstudiante(int dni) throws Exception {
        Estudiante e = estudianteRepository.findById(dni).orElseThrow(() -> new Exception("Estudiante no encontrado"));
        return new EstudianteDTO(e.getDni(), e.getNombre(), e.getApellido(), e.getLegajo());
    }



    public List<EstudianteDTO> getEstudiantesOrdenadosByApellido(){
        return estudianteRepository.getEstudiantesOrdenadosByApellido();
    }

    public Estudiante addEstudiante(Estudiante estudiante){
        return estudianteRepository.save(estudiante);
    }

    public void updateEstudiante(Estudiante estudiante) {
        estudianteRepository.updateEstudiante(estudiante.getNombre(), estudiante.getApellido(), estudiante.getLegajo(), estudiante.getDni());
    }

    public void deleteEstudiante(int dni) {
        estudianteRepository.deleteEstudiante(dni);
    }

    public EstudianteDTO getEstudianteByLegajo(int legajo) {
        return estudianteRepository.getEstudianteByLegajo(legajo);
    }

    public List<EstudianteDTO> getEstudiantesByGenero(String genero) {
        return estudianteRepository.getEstudiantesByGenero(genero);
    }

    public Estudiante getEstudianteByDni(int dni){
        return estudianteRepository.getEstudianteByDni(dni);
    }

}
