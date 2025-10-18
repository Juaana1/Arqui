package Arqui3.Entrega3.entity;
import jakarta.persistence.*;
import java.util.Date;

@Entity
public class Estudiante {
    @Id
    @Column(name="dni")
    private int dni;
    @Column(nullable = false)
    private String nombre;
    @Column(nullable=false)
    private String apellido;
    @Column
    private int edad;
    @Column(nullable=false)
    private String genero;
    @Column(nullable=false)
    private String ciudad;
    @Column
    private int legajo;


    public Estudiante() {

    }

    public Estudiante(String nombre, String apellido, int edad, String genero, int dni, String ciudad, int legajo) {
        this.nombre = nombre;
        this.apellido = apellido;
        this.edad = edad;
        this.genero = genero;
        this.dni = dni;
        this.ciudad = ciudad;
        this.legajo = legajo;
    }

    public String getNombre() {
        return nombre;
    }

    public void setNombre(String nombre) {
        this.nombre = nombre;
    }

    public String getApellido() {
        return apellido;
    }

    public void setApellido(String apellido) {
        this.apellido = apellido;
    }

    public int getEdad() {
        return edad;
    }

    public void setEdad(int edad) {
        this.edad = edad;
    }

    public String getGenero() {
        return genero;
    }

    public void setGenero(String genero) {
        this.genero = genero;
    }

    public int getDni() {
        return dni;
    }

    public void setDni(int dni) {
        this.dni = dni;
    }

    public String getCiudad() {
        return ciudad;
    }

    public void setCiudad(String ciudad) {
        this.ciudad = ciudad;
    }

    public int getLegajo() {
        return legajo;
    }

    public void setLegajo(int legajo) {
        this.legajo = legajo;
    }


    @Override
    public String toString() {
        return "Estudiante{" +
                " nombre='" + nombre + '\'' +
                ", apellido='" + apellido + '\'' +
                ", edad=" + edad +
                ", genero='" + genero + '\'' +
                ", dni=" + dni +
                ", ciudad='" + ciudad + '\'' +
                ", legajo=" + legajo +
                '}';
    }
}
