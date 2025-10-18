package Arqui3.Entrega3.dto;

public class EstudianteDTO {
    private int dni;
    private String nombre;
    private String apellido;
    private int legajo;
    private String genero;
    private String ciudad;
    private String carrera;

    public EstudianteDTO(int dni, String nombre, String apellido, int legajo, String ciudad, String carrera, String genero) {
        this.dni = dni;
        this.nombre = nombre;
        this.apellido = apellido;
        this.legajo = legajo;
        this.ciudad = ciudad;
        this.carrera = carrera;
        this.genero = genero;
    }

    public EstudianteDTO(int dni, String nombre, String apellido, int legajo) {
        this.dni = dni;
        this.nombre = nombre;
        this.apellido = apellido;
        this.legajo = legajo;
    }

    public EstudianteDTO(int dni, String nombre, String apellido, int legajo, String genero) {
        this.dni = dni;
        this.nombre = nombre;
        this.apellido = apellido;
        this.legajo = legajo;
        this.genero = genero;
    }

    public String getGenero() {
        return genero;
    }

    public String getNombre() {
        return nombre;
    }

    public int getDni() {
        return dni;
    }

    public String getApellido() {
        return apellido;
    }

    public int getLegajo() {
        return legajo;
    }

    @Override
    public String toString() {
        return "{" +
                " nombre=" + nombre +
                ", apellido=" + apellido +
                ", dni=" + dni +
                ", legajo=" + legajo +
           '}';
    }

    public String imprimirConGenero(){
        return "{" +
                " nombre=" + nombre +
                ", apellido=" + apellido +
                ", dni=" + dni +
                ", legajo=" + legajo +
                ", genero=" + genero +
                '}';
    }

    public String imprimirConCiudadCarrera(){
        return "{" +
                " nombre=" + nombre +
                ", apellido=" + apellido +
                ", dni=" + dni +
                ", legajo=" + legajo +
                ", genero=" + genero +
                ", ciudad=" +  ciudad +
                ", carrera=" + carrera +
                '}';
    }
}
