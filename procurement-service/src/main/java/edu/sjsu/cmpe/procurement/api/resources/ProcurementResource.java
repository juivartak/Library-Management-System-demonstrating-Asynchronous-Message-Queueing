package edu.sjsu.cmpe.procurement.api.resources;

import java.util.ArrayList;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.fusesource.stomp.jms.StompJmsConnectionFactory;
import org.fusesource.stomp.jms.StompJmsDestination;
import org.fusesource.stomp.jms.message.StompJmsMessage;
//import org.json.JSONObject;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import edu.sjsu.cmpe.procurement.domain.Jobs;

public class ProcurementResource extends Jobs{
	
    private static String user, password, host, queue, destination, ISBN;
    private static int port;
    //public static ArrayList<String> booksInQueue = new ArrayList <String>();
   
    public static ArrayList<String> booksInQueue = new ArrayList <String>();
   // public static ArrayList<String> topics = new ArrayList <String>();
    
	public void doJob() throws JMSException, JSONException
	{
		user = "admin";
		password = "password";
		host = "54.215.210.214";
		port = 61613;
		queue = "/queue/65880.book.orders";
		destination = arg(0, queue);
		
		StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
		factory.setBrokerURI("tcp://" + host + ":" + port);

		 System.out.println("" + host + " " +port);
	     System.out.println("Inside checkForMessage loop");
		
		Connection connection = factory.createConnection(user, password);
		System.out.println("Connection created");
		  
		connection.start();
		System.out.println("Connected");
		
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Destination dest = new StompJmsDestination(destination);
		
		MessageConsumer consumer = session.createConsumer(dest);
		System.out.println("Waiting for messages from " + queue + "...");
		long waitUntil=5000;
		
		while(true) 
		{
			Message msg = consumer.receive(waitUntil);
		    if( msg instanceof TextMessage ) 
		    {
		    	String body = ((TextMessage) msg).getText();
		    	//System.out.println(""+body);
		    	String []parts= body.split(":");
		    	//System.out.println("" +parts.length);
		    	for(int i=1;i<parts.length;i++)
		    	{
		    		//libraryName=parts[i];
			       	ISBN = parts[i];
			       	i++;
			       	booksInQueue.add(ISBN);
		    	}
		    	//ISBN =parts[1];
		    	//booksInQueue.add(ISBN);
		       	System.out.println("" + ISBN);
		    	System.out.println("Received message = " + body);
		    	if(booksInQueue.size()!=0)
				{
					sendPost();
					//getMethod();	
				}
		    } 
		    else if (msg instanceof StompJmsMessage) 
		    {
		    	StompJmsMessage smsg = ((StompJmsMessage) msg);
		    	String body = smsg.getFrame().contentAsString();
		    	String[] parts= body.split(":");
		    	//System.out.println("" +parts.length);
		    	//String ISBN = parts[1];
		    	for(int i=1;i<=parts.length;i++)
		    	{
		    		//parts= body.split(":");
		    		//libraryName=parts[i];
			       	ISBN = parts[i];
			       	i++;
			       	booksInQueue.add(ISBN);
		    	}
		    	//booksInQueue.add(ISBN);
		    	System.out.println("Received message = " + body);
		    	booksInQueue.add(msg.getStringProperty(body));
		    	{
					sendPost();
					//getMethod();	
				}
		    }
		   else if (msg ==null)
		    {
		    	System.out.println("No new messages. Exiting due to timeout-" +waitUntil/1000+"sec");
		    	getMethod();
		    	break;
		    } 
		    else
		    {
		    	System.out.println("Unexpected message type: "+msg.getClass());
		    }
		    
		}
		connection.close();
		System.out.println("Done");
	}

	private static String arg(int index, String defaultValue) {
	    return defaultValue;
	}
	  
	  public void sendPost() throws JMSException, JSONException 
	  {
		  //System.out.println("Queue size:" +booksInQueue.size());
			 Client client = Client.create();
		     WebResource webResource = client.resource("http://54.215.210.214:9000/orders");
		    // System.out.println("Inside sendPost");
		     ArrayList <Integer> bookISBN= new ArrayList <Integer>();
		     for (int i = 0; i < booksInQueue.size(); i++) 
		     {
		    	 bookISBN.add(Integer.parseInt(booksInQueue.get(i)));
		     }
		     String input = "{\"id\":\"65880\",\"order_book_isbns\": ".concat("[" + bookISBN.toString() + "]" + "}");
		     System.out.println("" +input);	
		     ClientResponse response = webResource.type("application/json").post(ClientResponse.class, input);
		     if (response.getStatus() != 200) 
		     { 
		    	 throw new RuntimeException("Failed : HTTP error code ***: " + response.getStatus());
		     }
		     System.out.println("Output from Server .... \n");
		     String output = response.getEntity(String.class);
		     System.out.println(output);
		     getMethod();
		    //getMethod1();
		    }
	  
	  public void getMethod() throws JMSException, JSONException 
	  {
	    	Client client = Client.create(); 
		    WebResource webResource = client.resource("http://54.215.210.214:9000/orders/65880");
		    ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);  
		    System.out.println("Got following books from Publisher...");
		   String output= response.getEntity(String.class);	
		    System.out.println("" +output);
		    sendToTopic(output);
	  }
	  
	  public void sendToTopic(String tempMsg) throws JMSException, JSONException 
	  {
		  	user = "admin";
			password = "password";
			host= "54.215.210.214";
			int port = 61613;
			Topic topic;

			StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
			factory.setBrokerURI("tcp://" + host + ":" + port);
			//System.out.println("Inside sendToTopic loop. tempMsg is " +tempMsg);

			Connection connection = factory.createConnection(user, password);
			connection.start();
			Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			
			JSONObject jObj= new JSONObject(tempMsg);
			//JSONArray jsonArray = new JSONArray(jObj);
			JSONArray jsonArray = new JSONArray();
			jsonArray= jObj.getJSONArray("shipped_books");
			
			//System.out.println(""+jsonArray.getString(0));
			for (int i=0;i<jsonArray.length();i++)
			{
				String category = jsonArray.getJSONObject(i).getString("category");
				//System.out.println("Category is:" +category);
				
				topic = session.createTopic("65880.book."+category);
				//System.out.println(""+topic);
				String data = ""+jsonArray.getJSONObject(i).getString("isbn")+":"+jsonArray.getJSONObject(i).getString("title")+":"+jsonArray.getJSONObject(i).getString("category")+":"+jsonArray.getJSONObject(i).getString("coverimage");
				MessageProducer producer = session.createProducer(topic);
				producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
				//TextMessage msg = session.createTextMessage(jObj.toString());
				TextMessage msg = session.createTextMessage(data);
				msg.setLongProperty("id", System.currentTimeMillis());
				producer.send(msg);
				System.out.println("Message is: "+msg);
			}		
			connection.close();
	  }
}

