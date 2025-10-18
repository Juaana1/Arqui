package Arqui3.Entrega3.dto;

import lombok.Getter;

public class EstudianteCarreraDTO {
        @Getter
        private int dni;
        @Getter
        private int idCarrera;
        @Getter
        private int antiguedad;
        @Getter
        private boolean esGraduado;
        @Getter
        private int fechaInscripcion;
        @Getter
        private int fechaEgreso;

        public EstudianteCarreraDTO(int dni, int idCarrera, int antiguedad, boolean esGraduado, int fechaInscripcion, int fechaEgreso) {
            this.dni = dni;
            this.idCarrera = idCarrera;
            this.antiguedad = antiguedad;
            this.esGraduado = esGraduado;
            this.fechaInscripcion = fechaInscripcion;
            this.fechaEgreso = fechaEgreso;
        }

        public EstudianteCarreraDTO() {

        }

        public boolean getEsGraduado() {
            return esGraduado;
        }

        @Override
        public String toString() {
            return "EstudianteCarrera{" +
                    "estudiante=" + dni +
                    ", idCarrera=" + idCarrera +
                    ", antiguedad=" + antiguedad +
                    ", esGraduado=" + esGraduado +
                    '}';
        }
    }