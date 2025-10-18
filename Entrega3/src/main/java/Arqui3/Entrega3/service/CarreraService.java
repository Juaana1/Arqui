package Arqui3.Entrega3.service;

import Arqui3.Entrega3.dto.CarreraDTO;
import Arqui3.Entrega3.dto.RegistroCarrerasDTO;
import Arqui3.Entrega3.entity.Carrera;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import Arqui3.Entrega3.repository.CarreraRepository;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

@Service
public class CarreraService {

    @Autowired
    private CarreraRepository carreraRepository;

    public Carrera getCarrera(Long id) {
        return carreraRepository.getCarrera(id);
    }

    public List<CarreraDTO> getCarreras() {
        return carreraRepository.getCarreras();
    }

    public Carrera addCarrera(Carrera carrera) {
        return carreraRepository.save(carrera);
    }

    public void updateCarrera(String nombre, Long id) {
        carreraRepository.updateCarrera(nombre, id);
    }

    public void deleteCarrera(Long carrera) {
        carreraRepository.deleteCarrera(carrera);
    }

}
