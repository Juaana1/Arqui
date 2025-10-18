package Arqui3.Entrega3.entity;

import jakarta.persistence.Embeddable;

import java.io.Serializable;
import java.util.Objects;


public class EstudianteCarreraId implements Serializable {
    private int estudiante;
    private int carrera;

    public EstudianteCarreraId() {
    }

    public EstudianteCarreraId(int estudiante, int carrera) {
        this.estudiante = estudiante;
        this.carrera = carrera;
    }

    public int getEstudiante() {
        return estudiante;
    }

    public int getCarrera() {
        return carrera;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EstudianteCarreraId that = (EstudianteCarreraId) o;
        return Objects.equals(estudiante, that.estudiante) && Objects.equals(carrera, that.carrera);
    }

    @Override
    public int hashCode() {
        return Objects.hash(estudiante, carrera);
    }

}