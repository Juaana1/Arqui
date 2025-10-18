package Arqui3.Entrega3.dto;

import lombok.Getter;

@Getter
public class CarreraDTO {
    @Getter
    private String nombreCarrera;
    @Getter
    private Long cantidadInscriptos;

    public CarreraDTO(String nombreCarrera, Long cantidadInscriptos) {
        this.nombreCarrera = nombreCarrera;
        this.cantidadInscriptos = cantidadInscriptos;
    }

    public CarreraDTO(String nombreCarrera) {
        this.nombreCarrera = nombreCarrera;
    }

    public void setNombreCarrera(String nombreCarrera) {
        this.nombreCarrera = nombreCarrera;
    }

    public void setCantidadInscriptos(Long cantidadInscriptos) {
        this.cantidadInscriptos = cantidadInscriptos;
    }

    @Override
    public String toString() {
        return "CarreraInscriptosDTO{" +
                "nombreCarrera='" + nombreCarrera + '\'' +
                ", cantidadInscriptos=" + cantidadInscriptos +
                '}';
    }
}
