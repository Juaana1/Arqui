package repository;

import entity.Carrera;

import java.util.List;

public interface CarreraRepository {

    Carrera getCarrera(Carrera carrera);
    List<Carrera> getCarreras();
    void addCarrera(Carrera carrera);
    void updateCarrera(Carrera carrera);
    void deleteCarrera(Long carrera);
}
