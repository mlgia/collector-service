package es.accenture.mlgia.service.impl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import es.accenture.mlgia.dto.Parking;
import es.accenture.mlgia.service.DatasetService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class DatasetServiceImpl implements DatasetService {
	
	@Value("${resource.csv}")
	private String resource;
	
	@Value("${output.file}")
	private String outputFile;

	@Scheduled(fixedDelay = 60000)
	@Override
	public void getDataset() {

		long timestamp = System.currentTimeMillis();
		log.info("Start getDataset: {}", timestamp);
		
		try {
			URL url = new URL(resource);
			BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream(), "UTF-8"));
			
			String content = reader.lines()
					.skip(1)
					.map(line -> lineToParking(line, timestamp))
					.map(parking -> parkingToLine(parking))
					.reduce((line1, line2) -> line1 + "\n" + line2)
					.orElse("");
			
			reader.close();
			
			log.info("Output: \n{}", content);
			
			write(content, timestamp);
			
		} catch (IOException e) {
			log.error(e.getMessage(), e);
		}
	}
	
	private void write(String content, Long timestamp) {
	    BufferedWriter writer;
		try {
			writer = new BufferedWriter(new FileWriter(String.format(outputFile, timestamp)));
		    writer.write(content);

		    writer.close();
		} catch (IOException e) {
			log.error(e.getMessage(), e);
		}
	     
	}
	
	private Parking lineToParking(String line, Long timestamp) {
		  line = line.replaceAll("\"", "");
		  String[] p = line.split(",");
		  
		  Parking item = Parking.builder()
				  .poiID(getLong(p[0]))
				  .nombre(getString(p[1]))
				  .direccion(getString(p[2]))
				  .telefono(getString(p[3]))
				  .correoelectronico(getString(p[4]))
				  .latitude(getDouble(p[5]))
				  .longitude(getDouble(p[6]))
				  .altitud(getDouble(p[7]))
				  .capacidad(getInteger(p[8]))
				  .capacidad_discapacitados(getString(p[9]))
				  .fechahora_ultima_actualizacion(getString(p[10]))
				  .libres(getInteger(p[11]))
				  .libres_discapacitados(getString(p[12]))
				  .nivelocupacion_naranja(getInteger(p[13]))
				  .nivelocupacion_rojo(getInteger(p[14]))
				  .smassa_sector_sare(getString(p[15]))
				  .timestamp(timestamp)
				  .build();
		  
		  return item;
	}
	
	private String parkingToLine(Parking parking) {
		
		StringBuilder csvLine = new StringBuilder();
		
		csvLine.append(parking.getPoiID()).append(",")
		.append(parking.getNombre()).append(",")
		.append(parking.getDireccion()).append(",")
		.append(parking.getTelefono()).append(",")
		.append(parking.getCorreoelectronico()).append(",")
		.append(parking.getLatitude()).append(",")
		.append(parking.getLongitude()).append(",")
		.append(parking.getAltitud()).append(",")
		.append(parking.getCapacidad()).append(",")
		.append(parking.getCapacidad_discapacitados()).append(",")
		.append(parking.getFechahora_ultima_actualizacion()).append(",")
		.append(parking.getLibres()).append(",")
		.append(parking.getLibres_discapacitados()).append(",")
		.append(parking.getNivelocupacion_naranja()).append(",")
		.append(parking.getNivelocupacion_rojo()).append(",")
		.append(parking.getTimestamp());

		return csvLine.toString();
	}
	
	private Long getLong(String word) {
		Long l = null;
		
		if (word != null && !word.isEmpty() && !word.equals("None")) {
			l = Long.valueOf(word.trim());
		}
		
		return l;
	}
	
	private Double getDouble(String word) {
		Double d = null;
		
		if (word != null && !word.isEmpty() && !word.equals("None")) {
			d = Double.valueOf(word);
		}
		
		return d;
	}
	
	private Integer getInteger(String word) {
		Integer i = null;
		if (word != null && !word.isEmpty() && !word.equals("None")) {
			i = Integer.decode(word.trim());
		}
		
		return i;
	}
	
	private String getString(String word) {
		String s = null;
		if (word != null && !word.isEmpty()) {
			s = word.trim();
		}
		
		return s;
		
	}

}
