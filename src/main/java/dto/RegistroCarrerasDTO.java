package dto;

public class RegistroCarrerasDTO {

    private String nombreCarrera;
    private int anio;
    private Long cantInscriptos;
    private Long cantEgresados;

    public RegistroCarrerasDTO(String nombreCarrera, int anio, Long cantInscriptos, Long cantEgresados) {
        this.nombreCarrera = nombreCarrera;
        this.cantInscriptos = cantInscriptos;
        this.cantEgresados = cantEgresados;
        this.anio = anio;
    }

    public String getNombreCarrera() {
        return nombreCarrera;
    }

    public void setNombreCarrera(String nombreCarrera) {
        this.nombreCarrera = nombreCarrera;
    }

    public int getAnio() {
        return anio;
    }

    public void setAnio(int anio) {
        this.anio = anio;
    }

    public Long getCantInscriptos() {
        return cantInscriptos;
    }

    public void setCantInscriptos(Long cantInscriptos) {
        this.cantInscriptos = cantInscriptos;
    }

    public Long getCantEgresados() {
        return cantEgresados;
    }

    public void setCantEgresados(Long cantEgresados) {
        this.cantEgresados = cantEgresados;
    }

    @Override
    public String toString() {
        return "nombreCarrera='" + nombreCarrera + '\'' +
                ", anio=" + anio +
                ", cantInscriptos=" + cantInscriptos +
                ", cantEgresados=" + cantEgresados +
                '}';
    }
}
