package es.accenture.mlgia.service.impl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import com.ibm.cloud.objectstorage.ClientConfiguration;
import com.ibm.cloud.objectstorage.SDKGlobalConfiguration;
import com.ibm.cloud.objectstorage.auth.AWSCredentials;
import com.ibm.cloud.objectstorage.auth.AWSStaticCredentialsProvider;
import com.ibm.cloud.objectstorage.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.ibm.cloud.objectstorage.oauth.BasicIBMOAuthCredentials;
import com.ibm.cloud.objectstorage.services.s3.AmazonS3;
import com.ibm.cloud.objectstorage.services.s3.AmazonS3Builder;
import com.ibm.cloud.objectstorage.services.s3.AmazonS3ClientBuilder;
import com.ibm.cloud.objectstorage.services.s3.model.ObjectMetadata;

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
	
	@Value("${output.file.name}")
	private String outputFileName;
	
	@Value("${ibm.cloud.object.storage.bucket}")
	private String ibmCloudStorageBucket;
	
	
	@Autowired
	private AmazonS3 cos;
	
	private static final String EMPTY = "";
	
	private static final String NONE = "None";

	private static final String COMMA_SEPARATOR = ",";

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
					.map(this::parkingToLine)
					.reduce((line1, line2) -> line1 + "\n" + line2)
					.orElse(EMPTY);
			
			reader.close();
			
			log.info("Output: \n{}", content);
			
			write(content, timestamp);
			writeToCloudStorage(content, timestamp);
			
		} catch (IOException e) {
			log.error(e.getMessage(), e);
		}
	}
	
	private void write(String content, Long timestamp) {
	    BufferedWriter writer;
		try {
			String path = outputFile.concat(String.format(outputFileName, timestamp));
			
			writer = new BufferedWriter(new FileWriter(path));
		    writer.write(content);

		    writer.close();
		} catch (IOException e) {
			log.error(e.getMessage(), e);
		}
	     
	}
	
	private void writeToCloudStorage(String content, Long timestamp) {
		
		InputStream stream = new ByteArrayInputStream(content.getBytes());
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentType("application/x-java-serialized-object");
		metadata.setContentLength(content.getBytes().length);
		
		cos.putObject(
				ibmCloudStorageBucket,
				String.format(outputFileName, timestamp),
				stream,
				metadata);
		
		
		
	}
	
	private Parking lineToParking(String line, Long timestamp) {
		  line = line.replaceAll("\"", "");
		  String[] p = line.split(COMMA_SEPARATOR);
		  
		  return Parking.builder()
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
		  
	}
	
	private String parkingToLine(Parking parking) {
		
		StringBuilder csvLine = new StringBuilder();
		
		csvLine.append(parking.getPoiID()).append(COMMA_SEPARATOR)
		.append(parking.getNombre()).append(COMMA_SEPARATOR)
		.append(parking.getDireccion()).append(COMMA_SEPARATOR)
		.append(parking.getTelefono()).append(COMMA_SEPARATOR)
		.append(parking.getCorreoelectronico()).append(COMMA_SEPARATOR)
		.append(parking.getLatitude()).append(COMMA_SEPARATOR)
		.append(parking.getLongitude()).append(COMMA_SEPARATOR)
		.append(parking.getAltitud()).append(COMMA_SEPARATOR)
		.append(parking.getCapacidad()).append(COMMA_SEPARATOR)
		.append(parking.getCapacidad_discapacitados()).append(COMMA_SEPARATOR)
		.append(parking.getFechahora_ultima_actualizacion()).append(COMMA_SEPARATOR)
		.append(parking.getLibres()).append(COMMA_SEPARATOR)
		.append(parking.getLibres_discapacitados()).append(COMMA_SEPARATOR)
		.append(parking.getNivelocupacion_naranja()).append(COMMA_SEPARATOR)
		.append(parking.getNivelocupacion_rojo()).append(COMMA_SEPARATOR)
		.append(parking.getTimestamp());

		return csvLine.toString();
	}
	
	private Long getLong(String word) {
		Long l = null;
		
		if (!StringUtils.isEmpty(word) && !word.equals(NONE)) {
			l = Long.valueOf(word.trim());
		}
		
		return l;
	}
	
	private Double getDouble(String word) {
		Double d = null;
		
		if (!StringUtils.isEmpty(word) && !word.equals(NONE)) {
			d = Double.valueOf(word);
		}
		
		return d;
	}
	
	private Integer getInteger(String word) {
		Integer i = null;
		if (!StringUtils.isEmpty(word) && !word.equals(NONE)) {
			i = Integer.decode(word.trim());
		}
		
		return i;
	}
	
	private String getString(String word) {
		String s = null;
		if (!StringUtils.isEmpty(word)) {
			s = word.trim();
		}
		
		return s;
		
	}

}
