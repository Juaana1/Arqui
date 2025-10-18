package Arqui3.Entrega3.entity;

import jakarta.persistence.*;

@Entity
public class Carrera {

    @Id
    @Column(name="ID_Carrera")
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int id;
    @Column(nullable = false)
    private String Nombre;

    public Carrera() {

    }

    public Carrera( String nombre) {
        Nombre = nombre;
    }

    public String getNombre() {
        return Nombre;
    }

    public void setNombre(String nombre) {
        Nombre = nombre;
    }

    public int getId() {
        return id;
    }

    @Override
    public String toString() {
        return "Carrera{" +
                "id=" + id +
                ", Nombre='" + Nombre + '\'' +
                '}';
    }
}
