package entity;

import jakarta.persistence.*;

@Entity
public class EstudianteCarrera {
    @Id
    @ManyToOne
    @JoinColumn(name="dni")
    private Estudiante estudiante;
    @Id
    @ManyToOne
    @JoinColumn(name="ID_Carrera")
    private Carrera Carrera;
    @Column
    private int antiguedad;
    @Column(nullable = false)
    private boolean esGraduado;
    @Column
    private int fechaInscripcion;
    @Column
    private int fechaEgreso;

    public EstudianteCarrera(int antiguedad, boolean esGraduado, int fechaInscripcion, int fechaEgreso, Carrera Carrera, Estudiante estudiante) {
        this.antiguedad = antiguedad;
        this.esGraduado = esGraduado;
        this.fechaInscripcion = fechaInscripcion;
        this.fechaEgreso = fechaEgreso;
        this.Carrera = Carrera;
        this.estudiante = estudiante;
    }

    public EstudianteCarrera() {

    }
    public int getFechaEgreso() {
        return fechaEgreso;
    }

    public int getFechaInscripcion() {
        return fechaInscripcion;
    }

    public int getAntiguedad() {
        return antiguedad;
    }

    public void setAntiguedad(int antiguedad) {
        this.antiguedad = antiguedad;
    }

    public Boolean getEsGraduado() {
        return esGraduado;
    }

    public void setEsGraduado(boolean esGraduado) {
        this.esGraduado = esGraduado;
    }

    public void setCarrera(Carrera carrera) {
        Carrera = carrera;
    }

    public void setEstudiante(Estudiante estudiante) {
        this.estudiante = estudiante;
    }

    @Override
    public String toString() {
        return "EstudianteCarrera{" +
                "estudiante=" + estudiante +
                ", idCarrera=" + Carrera +
                ", antiguedad=" + antiguedad +
                ", esGraduado=" + esGraduado +
                ", fechaInscripcion=" + fechaInscripcion +
                ", fechaEgreso=" + fechaEgreso +
                '}';
    }
}
