package edu.sjsu.cmpe.library;


import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;


import org.fusesource.stomp.jms.StompJmsConnectionFactory;
import org.fusesource.stomp.jms.StompJmsDestination;
import org.fusesource.stomp.jms.message.StompJmsMessage;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.views.ViewBundle;

import edu.sjsu.cmpe.library.api.resources.BookResource;
import edu.sjsu.cmpe.library.api.resources.RootResource;
import edu.sjsu.cmpe.library.config.LibraryServiceConfiguration;
import edu.sjsu.cmpe.library.domain.Book;
import edu.sjsu.cmpe.library.domain.Book.Status;
import edu.sjsu.cmpe.library.repository.BookRepository;
import edu.sjsu.cmpe.library.repository.BookRepositoryInterface;
import edu.sjsu.cmpe.library.ui.resources.HomeResource;

public class LibraryService extends Service<LibraryServiceConfiguration> {
	
    private final Logger log = LoggerFactory.getLogger(getClass());
    private static String user, password, host, queue, destination, libraryName, topic1, stompTopicName;
    private static int port;
    public static HashMap<Long, Book>BookMap = new HashMap<Long, Book>();
  
    public static void main(final String[] args) throws Exception {
    	
    	int numThreads =2;
    	ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    	Runnable backgroundTask = new Runnable(){

			public void run() {
			
				try {
					fetchMessage();
					
				} catch (Exception e) {
					e.printStackTrace();
				}	
			}
    	};
    	
    	Runnable primaryTask = new Runnable()
    	{
			public void run() {
				try {
					
					new LibraryService().run(args);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
    	};
   
    	executor.execute(primaryTask);
    	executor.execute(backgroundTask);	
    	
    }

    @Override
    public void initialize(Bootstrap<LibraryServiceConfiguration> bootstrap) {
	bootstrap.setName("library-service");
	bootstrap.addBundle(new ViewBundle());
    }

    private static String arg(int index, String defaultValue) {
    	    return defaultValue;
    	}

    @Override
    public void run(LibraryServiceConfiguration configuration,
	    Environment environment) throws Exception {
	// This is how you pull the configurations from library_x_config.yml
	String queueName = configuration.getStompQueueName();
	stompTopicName = configuration.getStompTopicName();
	log.debug("Queue name is {}. Topic name is {}", queueName,stompTopicName);
	libraryName= configuration.getLibraryName();
	
	// TODO: Apollo STOMP Broker URL and login
	
	user = "admin";
	password = "password";
	host = "54.215.210.214";
	port = 61613;
	queue = "/queue/65880.book.orders";
	destination = arg(0, queue);

	/** Root API */
	environment.addResource(RootResource.class);
	/** Books APIs */
	BookRepositoryInterface bookRepository = new BookRepository();
	environment.addResource(new BookResource(bookRepository));

	/** UI Resources */
	environment.addResource(new HomeResource(bookRepository));
    }
    
    public static void sendMessageToQueue(Long tempIsbn) throws JMSException
    {
    	StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
        factory.setBrokerURI("tcp://" + host + ":" + port);
        
       // System.out.println("Inside SendMessageToQueue loop");
        
        Connection connection = factory.createConnection(user, password);
        System.out.println("Connection created");
        
        connection.start();
        System.out.println("Connected");
        
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination dest = new StompJmsDestination(destination);
        MessageProducer producer = session.createProducer(dest);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        System.out.println("Sending messages to " +queue);
        String data = "" + libraryName + ":" + tempIsbn;
        TextMessage msg = session.createTextMessage(data);
        msg.setLongProperty("id", System.currentTimeMillis());
        producer.send(msg);

       // producer.send(session.createTextMessage("SHUTDOWN"));
        connection.close();  
    }
    
   public static void fetchMessage() throws JMSException, JSONException
    {
    	user = "admin";
    	password = "password";
    	host = "54.215.210.214";
    	port = 61613;
    	topic1 = "/topic/65880.book.*";
    	
    	destination = arg(0, topic1);

    	StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
    	factory.setBrokerURI("tcp://" + host + ":" + port);

    	Connection connection = factory.createConnection(user, password);
    	connection.start();
    	Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    	Destination dest = new StompJmsDestination(destination);

    	MessageConsumer consumer = session.createConsumer(dest);
    	System.currentTimeMillis();
    	System.out.println("Waiting for messages from" +topic1+"....");
    	long waitUntil=5000;
    	while(true) {
    	    Message msg = consumer.receive();
    	    if( msg instanceof TextMessage ) 
    	    {
    	    	String body = ((TextMessage) msg).getText();
    	    	System.out.println("Received message = " + body);
    	       	createBook(body);
    	    } 
    	    else if (msg instanceof StompJmsMessage) 
    	    {
    	    	StompJmsMessage smsg = ((StompJmsMessage) msg);
    	    	String body = smsg.getFrame().contentAsString();
    	    	System.out.println("Received message = " + body);
    	    	
    	    	createBook(body);
    	    } 
    	    else if (msg ==null)
		    {
		    	System.out.println("No new messages. Exiting due to timeout-" +waitUntil/1000+"sec");
		    	break;
		    }
    	    else 
    	    {
    	    	System.out.println("Unexpected message type: "+msg.getClass());
    	    }
    	   // connection.close();
    	   // System.out.println("Done");
    	}
    }
   
   private static void createBook(String tempBody) throws JSONException
   {
	   //System.out.println("inside createBook");
	   BookRepository br = new BookRepository();
	   String [] msgParts= tempBody.split(":");
	   String data = "{isbn:"+msgParts[0]+", title:"+msgParts[1]+", category:"+msgParts[2]+", coverImage:"+msgParts[3]+"}";
	  
	   JSONObject jObj = new JSONObject(data);
	   Book book = new Book();
	   book.setTitle(jObj.getString("title"));
	   book.setCategory(jObj.getString("category"));
	   //book.setCoverimage((URL) jObj.get("coverImage"));
	   book.setIsbn(jObj.getLong("isbn")); 
	   for(Book iter : BookRepository.bookMap.values())
	   {
		   if(BookRepository.bookMap.containsKey(iter.getIsbn()))
		   {
			   book.setStatus(Status.available);
		   }
		   br.saveBook(book);
	   }
   }
}

