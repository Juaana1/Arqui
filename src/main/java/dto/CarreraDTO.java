package dto;

public class CarreraDTO {
        private String nombreCarrera;
        private Long cantidadInscriptos;

        public CarreraDTO(String nombreCarrera, Long cantidadInscriptos) {
            this.nombreCarrera = nombreCarrera;
            this.cantidadInscriptos = cantidadInscriptos;
        }

        // Getters y setters
        public String getNombreCarrera() {
            return nombreCarrera;
        }

        public void setNombreCarrera(String nombreCarrera) {
            this.nombreCarrera = nombreCarrera;
        }

        public Long getCantidadInscriptos() {
            return cantidadInscriptos;
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
