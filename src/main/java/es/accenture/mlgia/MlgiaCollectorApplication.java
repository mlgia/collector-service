package es.accenture.mlgia;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.ibm.cloud.objectstorage.ClientConfiguration;
import com.ibm.cloud.objectstorage.SDKGlobalConfiguration;
import com.ibm.cloud.objectstorage.auth.AWSCredentials;
import com.ibm.cloud.objectstorage.auth.AWSStaticCredentialsProvider;
import com.ibm.cloud.objectstorage.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.ibm.cloud.objectstorage.oauth.BasicIBMOAuthCredentials;
import com.ibm.cloud.objectstorage.services.s3.AmazonS3;
import com.ibm.cloud.objectstorage.services.s3.AmazonS3ClientBuilder;

@EnableScheduling
@SpringBootApplication
public class MlgiaCollectorApplication {
	

	@Value("${ibm.cloud.object.storage.endpoint}")
	private String ibmCloudStorageEndpoint;
	
	@Value("${ibm.cloud.object.storage.api-key}")
	private String ibmCloudStorageApiKey;
	
	@Value("${ibm.cloud.object.storage.service-instance-id}")
	private String ibmCloudStorageServiceInstanceId;
	
	@Value("${ibm.cloud.object.storage.iam-endpoint}")
	private String ibmCloudStorageIamEndpoint;
	
	@Value("${ibm.cloud.object.storage.location}")
	private String ibmCloudStorageLocation;

	public static void main(String[] args) {
		SpringApplication.run(MlgiaCollectorApplication.class, args);
	}
	
	@Bean
	public AmazonS3 amazonS3() {
		
        SDKGlobalConfiguration.IAM_ENDPOINT = ibmCloudStorageIamEndpoint;
		
		AWSCredentials credentials = new BasicIBMOAuthCredentials(ibmCloudStorageApiKey, ibmCloudStorageServiceInstanceId);
		ClientConfiguration clientConfiguration = new ClientConfiguration().withRequestTimeout(5000);
		clientConfiguration.setUseTcpKeepAlive(true);
		
		return AmazonS3ClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(credentials))
				.withEndpointConfiguration(new EndpointConfiguration(ibmCloudStorageEndpoint, ibmCloudStorageLocation))
				.withPathStyleAccessEnabled(true)
				.withClientConfiguration(clientConfiguration)
				.build();
	}
	
}
