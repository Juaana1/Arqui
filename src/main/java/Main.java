import dto.CarreraDTO;
import dto.EstudianteDTO;
import dto.RegistroCarrerasDTO;
import entity.Carrera;
import entity.Estudiante;
import entity.EstudianteCarrera;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.EntityTransaction;
import jakarta.persistence.Persistence;
import repository.CarreraRepositoryImp;
import repository.EstudianteCarreraRepositoryImp;
import repository.EstudianteRepositoryImp;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        EntityManagerFactory emf = Persistence.createEntityManagerFactory("Integrador2");
        EntityManager em = emf.createEntityManager();
        EntityTransaction transaction = em.getTransaction();
        em.getTransaction().begin();

        EstudianteRepositoryImp estudianteRepository = new EstudianteRepositoryImp(em);
        CarreraRepositoryImp carreraRepository = new CarreraRepositoryImp(em);
        EstudianteCarreraRepositoryImp estudianteCarreraRepositoryImp = new EstudianteCarreraRepositoryImp(em);

        //Enunciado 2
        //a) dar de alta un estudiante
        Estudiante estudiante = new Estudiante("lucas","ferreiro",30,"Masculino",42777903,"vela", 20245);
        estudianteRepository.addEstudiante(estudiante);

        //b) matricular un estudiante en una carrera
        List<Carrera> carreras = new ArrayList<>();
        carreras = carreraRepository.getCarreras();
        Carrera carrera = carreraRepository.getCarrera(carreras.get(0));
        EstudianteCarrera estudianteCarrera = new EstudianteCarrera(4,false,2019,2026,carrera,estudiante);
        estudianteCarreraRepositoryImp.addEstudianteCarrera(estudianteCarrera);

        //c) recuperar todos los estudiantes, y especificar algún criterio de ordenamiento simple.
        List<EstudianteDTO> estudianteDTOSbyApellido = estudianteRepository.getEstudiantesOrdenadosByApellido();
        for (EstudianteDTO estudianteDTO : estudianteDTOSbyApellido) {
            System.out.println(estudianteDTO.toString());
        }

        //d) recuperar un estudiante, en base a su número de libreta universitaria.
        EstudianteDTO estudianteDTO = estudianteRepository.getEstudianteByLegajo(20245);
        System.out.println(estudianteDTO.toString());



        //e) recuperar todos los estudiantes, en base a su género.

        List<EstudianteDTO> estudianteDTOSbyGenero = estudianteRepository.getEstudiantesByGenero("Masculino");
        for (EstudianteDTO est : estudianteDTOSbyGenero) {
            System.out.println(est.imprimirConGenero());
        }


        //f) recuperar las carreras con estudiantes inscriptos, y ordenar por cantidad de inscriptos.
        List<CarreraDTO> estudianteCarreraDTOS = estudianteCarreraRepositoryImp.getCarrerasInscriptosOrdenadas();
        for (CarreraDTO carreraDTO : estudianteCarreraDTOS) {
            System.out.println(carreraDTO.toString());
        }

        //g) recuperar los estudiantes de una determinada carrera, filtrado por ciudad de resid
        List<EstudianteDTO> estudianteDTOSbyCiudad = estudianteCarreraRepositoryImp.getEstudiantesByCarreraFiltroCiudad("Derecho","Moreno");
        for (EstudianteDTO estudianteD : estudianteDTOSbyCiudad) {
            System.out.println(estudianteD.imprimirConCiudadCarrera());
        }

        System.out.println("------------------------------------------------------------------");

        //Enuncuado 3
        List<RegistroCarrerasDTO> registroCarrerasDTOS;
        registroCarrerasDTOS = estudianteCarreraRepositoryImp.reporteCarreras();
        for (RegistroCarrerasDTO dto : registroCarrerasDTOS) {
            System.out.println(dto.toString());
        }

        System.out.println("-------------------------------------------------------------------");

        transaction.commit();
        em.close();
        emf.close();
    }
}
