package org.ehrbase.repository;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.ehrbase.api.service.SystemService;
import org.ehrbase.service.TimeProvider;
import org.springframework.stereotype.Repository;
import java.util.Optional;
import java.util.UUID;
import com.nedap.archie.rm.composition.Composition;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Repository
public class CompositionRepositoryMongo {

    private final MongoDatabase mongoDatabase;
    private final ObjectMapper objectMapper;
    private final Logger logger = LoggerFactory.getLogger(CompositionRepositoryMongo.class);

    public CompositionRepositoryMongo() {
        // Initialize the MongoDB client and database connection
        var mongoClient = MongoClients.create("mongodb+srv://francesc:francesc@ist-shared.n0kts.mongodb.net/test");
        this.mongoDatabase = mongoClient.getDatabase("hc-playground");
        this.objectMapper = new ObjectMapper(); 
    }

    // Commit a new composition to MongoDB
    public void commit(UUID ehrId, Composition composition) {
        logger.info("Committing new composition to MongoDB for EHR ID: {}", ehrId);

        MongoCollection<Document> collection = mongoDatabase.getCollection("openehr-compositions");

        try {
            // Serialize the Composition object to JSON string
            String compositionJson = objectMapper.writeValueAsString(composition);

            // Convert the JSON string to a MongoDB Document
            Document compositionDocument = Document.parse(compositionJson);

            // Create a MongoDB document with EHR ID and the Composition document
            Document document = new Document("ehrId", ehrId.toString())
                    .append("composition", compositionDocument);  // Store as a structured document

            // Insert the document into MongoDB
            collection.insertOne(document);

            logger.info("Composition successfully inserted into MongoDB for EHR ID: {}", ehrId);
        } catch (Exception e) {
            logger.error("Error while committing composition to MongoDB for EHR ID: {}", ehrId, e);
            throw new RuntimeException("Failed to serialize or insert composition", e);
        }
    }

    // Retrieve composition by EHR ID
    public Optional<Composition> findByEhrId(UUID ehrId) {
        MongoCollection<Document> collection = mongoDatabase.getCollection("openehr-compositions");
        Document document = collection.find(new Document("ehrId", ehrId.toString())).first();
        if (document != null) {
            // Convert the document back to a Composition object
            Composition composition = new Composition(); // Adjust deserialization
            return Optional.of(composition);
        } else {
            return Optional.empty();
        }
    }
}