package com.techy.betterreadsdataloader;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.io.IOException;
import java.nio.file.Files;

import javax.annotation.PostConstruct;

import com.techy.betterreadsdataloader.connection.DataStaxAstraProperties;
import com.techy.betterreadsdataloader.model.Author;
import com.techy.betterreadsdataloader.model.AuthorRepository;
import com.techy.betterreadsdataloader.model.Book;
import com.techy.betterreadsdataloader.model.BookRepository;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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

	@Autowired
	private BookRepository bookRepository;

	@Value("${datadump.location.author}")
	private  String author_dump;

	@Value("${datadump.location.works}")
	private String works_dump;

	public static void main(String[] args) {
		SpringApplication.run(BetterReadsDataLoaderApplication.class, args);
	}

	private void initAuthors(){
		Path path =Paths.get(author_dump);
		try(Stream<String> lines =  Files.lines(path)){
			lines.forEach(line -> {
				// Read and parse the line
				String jsonString = line.substring(line.indexOf("{"));
				try {
					JSONObject jsonObj = new JSONObject(jsonString);
					// Construct author object
					Author author = new Author();
					author.setName(jsonObj.optString("name"));
					author.setPersonalName(jsonObj.optString("personal_name"));
					author.setId(jsonObj.optString("key").replace("/authors/", ""));

					//pesist using Repository
					System.out.println("Saving Author "+author.getName()+"..........");
					authorRepository.save(author);
 
				} catch (JSONException e) {
					e.printStackTrace();
				}
				
				

			});

		} catch(IOException exception){
			exception.printStackTrace();
		}
	}

	private void initWorks(){
		Path path =Paths.get(works_dump);
		DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
		try(Stream<String> lines =  Files.lines(path)){
			lines.forEach(line -> {
				// Read and parse the line
				String jsonString = line.substring(line.indexOf("{"));
				try {
					JSONObject jsonObj = new JSONObject(jsonString);
					// Construct author object
					Book book = new Book();
					book.setId(jsonObj.getString("key").replace("/works/", ""));
					book.setName(jsonObj.optString("title"));
					JSONObject descriptionObj = jsonObj.optJSONObject("description");
					if(descriptionObj!=null)
					{
						book.setDescription(descriptionObj.optString("value"));
					}
					
					JSONObject publishedObj = jsonObj.optJSONObject("created");
					if(publishedObj!=null)
					{
						String dateStr = publishedObj.getString("value");
						book.setPublishedDate(LocalDate.parse(dateStr,dateFormatter));

					}

					JSONArray coversJsonArray = jsonObj.optJSONArray("covers");
					if(coversJsonArray!=null)
					{
						List<String> coverIds = new ArrayList<>();
						for(int i=0;i<coversJsonArray.length();i++)
						{
							coverIds.add(coversJsonArray.getString(i));
						}
						book.setCoverIds(coverIds);
					}
					

					JSONArray authorsArray = jsonObj.optJSONArray("authors");
					if(authorsArray!=null)
					{
						List<String> authorIds = new ArrayList<>();
						for(int i=0;i<authorsArray.length();i++)
						{
							String authorId =authorsArray.getJSONObject(i).getJSONObject("author")
							.getString("key").replace("/authors/", "");
							authorIds.add(authorId);
						}
						book.setAuthorIds(authorIds);

						// get respective author names of author ids from author repo

						List<String> authorNames =  authorIds.stream().map(id -> authorRepository.findById(id))
						.map(optionalAuthor -> {
							if(!optionalAuthor.isPresent()) return "Unknown Author";
							return optionalAuthor.get().getName();
						}).collect(Collectors.toList());
						book.setAuthorNames(authorNames);
					}

					
					//pesist using Repository
					System.out.println("saving Book "+book.getName()+"...........");
					bookRepository.save(book);
					
 
				} catch (Exception e) {
					e.printStackTrace();
				}
				
				

			});

		} catch(IOException exception){
			exception.printStackTrace();
		}
	}

	@PostConstruct
	public void start()
	{
		System.out.println("BetterReadsDataLoaderApplication started....");
		initAuthors();
		initWorks();
	}


	@Bean
	public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties)
	{
		Path bundle = astraProperties.getSecureConnectBundle().toPath();
		return builder -> builder.withCloudSecureConnectBundle(bundle);
	}
}
