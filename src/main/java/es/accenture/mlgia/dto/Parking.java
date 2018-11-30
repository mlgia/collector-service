package es.accenture.mlgia.dto;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Parking implements Serializable {

	private static final long serialVersionUID = -621345867431367797L;
	private Long poiID;
	private String nombre;
	private String direccion;
	private String telefono;
	private String correoelectronico;
	private Double latitude;
	private Double longitude;
	private Double altitud;
	private Integer capacidad;
	private String capacidad_discapacitados;
	private String fechahora_ultima_actualizacion;
	private Integer libres;
	private String libres_discapacitados;
	private Integer nivelocupacion_naranja;
	private Integer nivelocupacion_rojo;
	private String smassa_sector_sare;
	private Long timestamp;

	
	
}
