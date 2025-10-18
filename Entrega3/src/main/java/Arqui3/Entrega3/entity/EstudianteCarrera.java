package Arqui3.Entrega3.entity;

import jakarta.persistence.*;
import lombok.Getter;

import java.io.Serializable;


@Entity
@IdClass(EstudianteCarreraId.class)
public class EstudianteCarrera implements Serializable {
    @Getter
    @Id
    @ManyToOne
    @JoinColumn(name="dni", nullable = false)
    private Estudiante estudiante;
    @Getter
    @Id
    @ManyToOne
    @JoinColumn(name="ID_Carrera", nullable = false)
    private Carrera carrera;
    @Getter
    @Column
    private int antiguedad;
    @Column(nullable = false)
    private boolean esGraduado;
    @Getter
    @Column
    private int fechaInscripcion;
    @Getter
    @Column
    private int fechaEgreso;

    public EstudianteCarrera(Estudiante estudiante, Carrera carrera, int antiguedad, boolean esGraduado, int fechaInscripcion, int fechaEgreso) {
        this.estudiante = estudiante;
        this.carrera = carrera;
        this.antiguedad = antiguedad;
        this.esGraduado = esGraduado;
        this.fechaInscripcion = fechaInscripcion;
        this.fechaEgreso = fechaEgreso;
    }

    public EstudianteCarrera() {

    }

    public boolean getEsGraduado() {
        return esGraduado;
    }

    @Override
    public String toString() {
        return "EstudianteCarrera{" +
                "estudiante=" + estudiante +
                ", idCarrera=" + carrera +
                ", antiguedad=" + antiguedad +
                ", esGraduado=" + esGraduado +
                ", fechaInscripcion=" + fechaInscripcion +
                ", fechaEgreso=" + fechaEgreso +
                '}';
    }
}
