package repository;

import dto.CarreraDTO;
import dto.EstudianteDTO;
import dto.RegistroCarrerasDTO;
import entity.Carrera;
import entity.Estudiante;
import entity.EstudianteCarrera;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import jakarta.persistence.TypedQuery;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class EstudianteCarreraRepositoryImp implements EstudianteCarreraRepository {
    private EntityManager em;

    public EstudianteCarreraRepositoryImp(EntityManager em) {
        this.em = em;
    }

    @Override
    public EstudianteCarrera getEstudianteCarrera(Estudiante estudiante, Carrera carrera) {
        TypedQuery<EstudianteCarrera> query = em.createQuery("SELECT ec FROM EstudianteCarrera ec WHERE ec.estudiante = :dni AND ec.Carrera = :ID_Carrera", EstudianteCarrera.class);
        query.setParameter("dni", estudiante.getDni());
        query.setParameter("ID_Carrera", carrera.getId());
        return query.getSingleResult();
    }

    @Override
    public List<EstudianteCarrera> getEstudiantesCarreras() {
        List<EstudianteCarrera> estudiantes;
        TypedQuery<EstudianteCarrera> query = em.createQuery("SELECT ec FROM EstudianteCarrera ec ",   EstudianteCarrera.class);
        estudiantes = query.getResultList();
        return estudiantes;
    }

    @Override
    public void addEstudianteCarrera(EstudianteCarrera estudianteCarrera) {
        //matricular un estudiante en una carrera (crear relación)
        em.persist(estudianteCarrera);
    }

    @Override
    public void updateCarrera(EstudianteCarrera estudianteCarrera) {
        em.merge(estudianteCarrera);
    }

    @Override
    public void deleteCarrera(Estudiante estudiante, Carrera carrera) {
        Query query = em.createQuery("DELETE FROM EstudianteCarrera ec WHERE ec.estudiante = :estudiante AND ec.Carrera = :carrera");
        query.setParameter("estudiante", estudiante);
        query.setParameter("carrera", carrera);
        query.executeUpdate();
    }

    @Override
    public List<EstudianteDTO> getEstudiantesByCarreraFiltroCiudad(String carrera, String ciudad) {
        List<EstudianteDTO> estudiantes;
        String jpql = "SELECT new dto.EstudianteDTO(ec.estudiante.dni, ec.estudiante.nombre, ec.estudiante.apellido, ec.estudiante.legajo, ec.estudiante.ciudad, ec.Carrera.nombre, ec.estudiante.genero) from EstudianteCarrera ec where ec.estudiante.ciudad=:ciudad and ec.Carrera.nombre=:carrera";
        Query query = em.createQuery(jpql, EstudianteDTO.class);
        query.setParameter("carrera", carrera);
        query.setParameter("ciudad", ciudad);
        estudiantes = query.getResultList();
        return estudiantes;
    }

    @Override
    public List<CarreraDTO> getCarrerasInscriptosOrdenadas() {
        String jpql = "SELECT new dto.CarreraDTO(c.nombre, COUNT(ec.Carrera.id)) " +
                "FROM EstudianteCarrera ec " +
                "JOIN Carrera c ON ec.Carrera.id = c.id " +
                "GROUP BY c.id, c.nombre " +
                "ORDER BY COUNT(ec.Carrera.id) DESC";

        Query query = em.createQuery(jpql, CarreraDTO.class);
        List<CarreraDTO> resultados = query.getResultList();
        return resultados;
    }

    private List<RegistroCarrerasDTO> getEgresadosByYear(){
        List<RegistroCarrerasDTO> reporte;
        String jpql = "SELECT new dto.RegistroCarrerasDTO(c.nombre,ec.fechaEgreso, null ,COUNT(e.dni)) FROM EstudianteCarrera ec " +
                "JOIN Carrera c ON ec.Carrera.id = c.id " +
                "JOIN Estudiante e ON ec.estudiante.dni = e.dni " +
                "GROUP BY c.nombre, ec.fechaEgreso " +
                "ORDER BY c.nombre, ec.fechaEgreso ";
        Query query = em.createQuery(jpql, RegistroCarrerasDTO.class);
        reporte = query.getResultList();
        return reporte;
    }

    private List<RegistroCarrerasDTO> getInscriptosByYear(){
        List<RegistroCarrerasDTO> reporte;
        String jpql = "SELECT new dto.RegistroCarrerasDTO(c.nombre,ec.fechaInscripcion, COUNT(e.dni) , null ) FROM EstudianteCarrera ec " +
                "JOIN Carrera c ON ec.Carrera.id = c.id " +
                "JOIN Estudiante e ON ec.estudiante.dni = e.dni " +
                "GROUP BY c.nombre, ec.fechaInscripcion " +
                "ORDER BY c.nombre, ec.fechaInscripcion ";
        Query query = em.createQuery(jpql, RegistroCarrerasDTO.class);
        reporte = query.getResultList();
        return reporte;
    }

    @Override
    public List<RegistroCarrerasDTO> reporteCarreras() {
        List<RegistroCarrerasDTO> reporteaInscriptos = this.getInscriptosByYear();
        List<RegistroCarrerasDTO> reporteaEgresados = this.getEgresadosByYear();

        List<RegistroCarrerasDTO> reporte = new ArrayList(reporteaInscriptos);

        for (int i = 0; i < reporteaEgresados.size(); i++) {
            for (int j = 0; j < reporteaInscriptos.size(); j++) {
                if ( reporteaEgresados.get(i).getNombreCarrera().equals(reporteaInscriptos.get(j).getNombreCarrera()) &&
                    reporteaEgresados.get(i).getAnio() == reporteaInscriptos.get(j).getAnio() ) {
                    reporte.get(j).setCantEgresados(reporteaEgresados.get(i).getCantEgresados());
                    reporteaEgresados.remove(i);
                    i--;
                }
            }
        }

        reporte.addAll(reporteaEgresados);
        reporte.sort(Comparator.comparing(RegistroCarrerasDTO::getNombreCarrera)
                .thenComparing(RegistroCarrerasDTO::getAnio));
        return reporte;
    }
}