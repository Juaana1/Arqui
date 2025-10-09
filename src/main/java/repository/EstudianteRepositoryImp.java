package repository;

import dto.EstudianteDTO;
import entity.Estudiante;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import jakarta.persistence.TypedQuery;

import java.util.List;

public class EstudianteRepositoryImp implements EstudianteRepository {
    private EntityManager em;

    public EstudianteRepositoryImp(EntityManager em) {
        this.em = em;
    }

    @Override
    public Estudiante getEstudiante(Estudiante estudiante) {
        TypedQuery<Estudiante> query = em.createQuery("SELECT e FROM Estudiante e WHERE e.dni = :dni", Estudiante.class);
        query.setParameter("dni", estudiante.getDni());
        return query.getSingleResult();
    }

    @Override
    public List<EstudianteDTO> getEstudiantesOrdenadosByApellido() {
        List<EstudianteDTO> estudiantes;
        String jpql = "SELECT new dto.EstudianteDTO(e.dni,e.nombre,e.apellido,e.legajo) FROM Estudiante e ORDER BY e.apellido ASC";
        Query query = em.createQuery(jpql, EstudianteDTO.class);
        estudiantes = query.getResultList();
        return estudiantes;
    }

    //Consigna dar de alta un estudiante
    @Override
    public void addEstudiante(Estudiante estudiante) {
        em.persist(estudiante);
    }

    @Override
    public void updateEstudiante(Estudiante estudiante) {
        em.merge(estudiante);
    }

    @Override
    public void deleteEstudiante(Estudiante estudiante) {
        Estudiante est = em.find(Estudiante.class, estudiante);
        if (est != null) {
            em.remove(est);
        }
    }

    @Override
    public EstudianteDTO getEstudianteByLegajo(int legajo) {
        EstudianteDTO estudiante;
        String jpql = "SELECT new dto.EstudianteDTO(e.dni, e.nombre, e.apellido, e.legajo) FROM Estudiante e WHERE e.legajo = :legajo";
        Query query = em.createQuery(jpql, EstudianteDTO.class);
        query.setParameter("legajo", legajo);
        estudiante = (EstudianteDTO) query.getSingleResult();
        return estudiante;
    }

    @Override
    public List<EstudianteDTO> getEstudiantesByGenero(String genero) {
        List<EstudianteDTO> estudiantes;
        String jpql = "SELECT new dto.EstudianteDTO(e.dni, e.nombre, e.apellido, e.legajo, e.genero) FROM Estudiante e WHERE e.genero = :genero";
        Query query = em.createQuery(jpql, EstudianteDTO.class);
        query.setParameter("genero", genero);
        estudiantes = query.getResultList();
        return estudiantes;
    }


}
