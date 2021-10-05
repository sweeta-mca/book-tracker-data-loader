package com.techy.betterreadsdataloader;

import java.nio.file.Path;

import javax.annotation.PostConstruct;

import com.techy.betterreadsdataloader.connection.DataStaxAstraProperties;
import com.techy.betterreadsdataloader.model.Author;
import com.techy.betterreadsdataloader.model.AuthorRepository;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BetterReadsDataLoaderApplication {

	@Autowired
	private AuthorRepository authorRepository;
	public static void main(String[] args) {
		SpringApplication.run(BetterReadsDataLoaderApplication.class, args);
	}

	@PostConstruct
	public void start()
	{
		System.out.println("BetterReadsDataLoaderApplication started....");
		Author author =new Author();
		author.setId("1234ID");
		author.setName("Kalam");
		author.setPersonalName("Abdul Kalam");
		authorRepository.save(author);
		
	}








	@Bean
	public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties)
	{
		Path bundle = astraProperties.getSecureConnectBundle().toPath();
		return builder -> builder.withCloudSecureConnectBundle(bundle);
	}
}
