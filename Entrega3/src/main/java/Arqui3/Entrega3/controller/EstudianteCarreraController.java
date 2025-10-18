package Arqui3.Entrega3.controller;

import Arqui3.Entrega3.dto.CarreraDTO;
import Arqui3.Entrega3.dto.EstudianteCarreraDTO;
import Arqui3.Entrega3.dto.EstudianteDTO;
import Arqui3.Entrega3.entity.Carrera;
import Arqui3.Entrega3.entity.Estudiante;
import Arqui3.Entrega3.entity.EstudianteCarrera;
import Arqui3.Entrega3.repository.CarreraRepository;
import Arqui3.Entrega3.repository.EstudianteCarreraRepository;
import Arqui3.Entrega3.repository.EstudianteRepository;
import Arqui3.Entrega3.service.EstudianteCarreraService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("api/estudianteCarreras")

public class EstudianteCarreraController {
    @Autowired
    private EstudianteCarreraService estudianteCarreraService;
    @Autowired
    private EstudianteRepository estudianteRepository;
    @Autowired
    private CarreraRepository carreraRepository;
    @Autowired
    private EstudianteCarreraRepository estudianteCarreraRepository;

    @GetMapping
    public ResponseEntity<List<EstudianteCarreraDTO>> getEstudiantesCarreras() {
        List<EstudianteCarreraDTO> carreras = estudianteCarreraService.getEstudiantesCarreras();
        return ResponseEntity.ok(carreras);
    }

    @GetMapping("/ord/inscriptos")
    public ResponseEntity<List<CarreraDTO>> getCarrerasInscriptosOrdenadas() {
        List<CarreraDTO> carreras = estudianteCarreraService.getCarrerasInscriptosOrdenadas();
        return ResponseEntity.ok(carreras);
    }

    @GetMapping("/estudianteByCarreraYCiudad")
    public ResponseEntity<List<EstudianteDTO>> getEstudiantesByCarreraFiltroCiudad(
            @RequestParam("Carrera") long id,
            @RequestParam("Ciudad") String ciudad) {

        List<EstudianteDTO> estudiantes = estudianteCarreraService.getEstudiantesByCarreraFiltroCiudad(id, ciudad);
        return ResponseEntity.ok(estudiantes);
    }

  /*  @GetMapping("/reporte")
    public ResponseEntity<RegistroCarrerasDTO> getReporteCarreras() {
        RegistroCarrerasDTO rc = estudianteCarreraService.getReporteCarreras();
        return ResponseEntity.ok(rc);
    }*/

    @PostMapping
    public ResponseEntity<EstudianteCarrera> crearEstudianteCarrera(@RequestBody EstudianteCarreraDTO request) {
        // Buscamos los objetos de Estudiante y Carrera por sus IDs
        Estudiante estudiante = estudianteRepository.findById(request.getDni())
                .orElseThrow(() -> new RuntimeException("Estudiante no encontrado"));
        Carrera carrera = carreraRepository.findById(request.getIdCarrera())
                .orElseThrow(() -> new RuntimeException("Carrera no encontrada"));
        EstudianteCarrera estudianteCarrera = new EstudianteCarrera(
                estudiante,
                carrera,
                request.getAntiguedad(),
                request.getEsGraduado(),
                request.getFechaInscripcion(),
                request.getFechaEgreso()
        );
         if(estudianteCarreraRepository.getEstudianteCarrera(estudiante, carrera) == null) {
             EstudianteCarrera nuevaRelacion = estudianteCarreraRepository.save(estudianteCarrera);
             return ResponseEntity.ok(nuevaRelacion);
         }
         throw new RuntimeException("Estudiante carrera ya existe");
    }

    @GetMapping("/report")
    public ResponseEntity<?> reporteCarreras() {
        try {
            return ResponseEntity.status(HttpStatus.ACCEPTED).body(estudianteCarreraService.reporteCarreras());
        } catch (
                Exception e) {
            e.printStackTrace();
            String errorJson = "{\"message\": \"Error en la consulta\", \"details\"}";
            return ResponseEntity
                    .status(HttpStatus.BAD_REQUEST)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(errorJson);
        }

    }
}
