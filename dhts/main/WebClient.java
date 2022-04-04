package edu.stevens.cs549.dhts.main;

import java.net.URI;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import org.glassfish.jersey.jackson.JacksonFeature;

import edu.stevens.cs549.dhts.activity.DHTBase;
import edu.stevens.cs549.dhts.activity.NodeInfo;
import edu.stevens.cs549.dhts.resource.TableRep;

public class WebClient {
	
	private static final String TAG = WebClient.class.getCanonicalName();

	private Logger logger = Logger.getLogger(TAG);

	private void error(String msg, Exception e) {
		logger.log(Level.SEVERE, msg, e);
	}

	/*
	 * Encapsulate Web client operations here.
	 * 
	 * TODO: Fill in missing operations.
	 * Creation of client instances is expensive, so just create one.
	 */
	protected Client client;
	
	public WebClient() {
		client = ClientBuilder.newBuilder()
				.register(ObjectMapperProvider.class)
				.register(JacksonFeature.class)
				.build();
	}

	private void info(String mesg) {
		Log.weblog(TAG, mesg);
	}

	private Response getRequest(URI uri) {
		try {
			Response req = client.target(uri)
					.request() 
					.get();
			return req;
		} 
		catch (Exception e) {
			error("get request throwing error!", e);
			return null;
		}
	}

	// -- put request.. test output error message FIXME
	private Response putRequest(URI uri, TableRep tableRep) {
		try { 
		Response req = client.target(uri)
				.request(MediaType.APPLICATION_JSON_TYPE)
				.put(Entity.entity(tableRep, MediaType.APPLICATION_JSON_TYPE)); 
			return req; 
		} 
		catch (Exception e) { 
			error ("Put request throwing error!", e); 
			return null; 
		}
	}
	
	private Response putRequest(URI uri) {
		try {
			Response req = client.target(uri)
					.request()
					.put(Entity.text(""));
			return req;
		} 
		catch (Exception e) {
			error("Put request throwing error!", e); // check exception FIXME
			return null;
		}
	}


	private Response deleteRequest (URI uri) { 
		try { 
			Response req = client.target(uri)
					.request()
					.delete(); 
			return req; 
		} 
		catch (Exception e) { 
			error ("delete request throwing error!", e); 
			return null; 
		}
	}

	/*
	 * Ping a remote site to see if it is still available.
	 */
	public boolean isFailed(URI base) {
		URI uri = UriBuilder.fromUri(base).path("info").build();
		Response c = getRequest(uri);
		return c.getStatus() >= 300;
	}

	/*
	 * Get the predecessor pointer at a node.
	 */
	public NodeInfo getPred(NodeInfo node) throws DHTBase.Failed {
		URI predPath = UriBuilder.fromUri(node.addr).path("pred").build();
		info("client getPred(" + predPath + ")");
		Response response = getRequest(predPath);
		if (response == null || response.getStatus() >= 300) {
			throw new DHTBase.Failed("GET /pred");
		} else {
			NodeInfo pred = response.readEntity(NodeInfo.class);
			return pred;
		}
	}


	
	public NodeInfo getSucc(NodeInfo node) throws DHTBase.Failed { 
		
		URI succPath = UriBuilder.fromUri(node.addr).path("succ").build(); 
		
		info("client getSucc(" + succPath + ")"); 
		Response response = getRequest(succPath); 
		if (response == null || response.getStatus() >= 300) { 
			throw new DHTBase.Failed("Get /succ"); 
		} 
		else { 
			NodeInfo succID = response.readEntity(NodeInfo.class); 
			return succID; 
		}
	}
	

	
	public NodeInfo getClosestPrecedingFinger (NodeInfo node, int id) throws DHTBase.Failed { 

		UriBuilder ub = UriBuilder.fromUri(node.addr); 
		URI closestPrec = ub.path("finger").queryParam("id", id).build(); 
		info("client closestPrecedingFinger("+ closestPrec + ")"); 
		Response response = getRequest(closestPrec); 
		if (response == null || response.getStatus() >= 300) { 
			throw new DHTBase.Failed("Get /finger?id=ID"); 
		} 
		else { 
			NodeInfo FV = response.readEntity(NodeInfo.class); 
			return FV; 
		}
	}

	/*
	 * Notify node that we (think we) are its predecessor.
	 */
	public TableRep notify(NodeInfo node, TableRep predDb) throws DHTBase.Failed {
		/*
		 * The protocol here is more complex than for other operations. We
		 * notify a new successor that we are its predecessor, and expect its
		 * bindings as a result. But if it fails to accept us as its predecessor
		 * (someone else has become intermediate predecessor since we found out
		 * this node is our successor i.e. race condition that we don't try to
		 * avoid because to do so is infeasible), it notifies us by returning
		 * null. This is represented in HTTP by RC=304 (Not Modified).
		 */
		NodeInfo thisNode = predDb.getInfo();
		UriBuilder ub = UriBuilder.fromUri(node.addr).path("notify");
		URI notifyPath = ub.queryParam("id", thisNode.id).build();
		info("client notify(" + notifyPath + ")");
		Response response = putRequest(notifyPath, predDb);
		if (response != null && response.getStatusInfo() == Response.Status.NOT_MODIFIED) {
			/*
			 * Do nothing, the successor did not accept us as its predecessor.
			 */
			return null;
		} else if (response == null || response.getStatus() >= 300) {
			throw new DHTBase.Failed("PUT /notify?id=ID");
		} else {
			TableRep bindings = response.readEntity(TableRep.class);
			return bindings;
		}
	}

	// TODO 
	/*
	 * Get bindings under a key.
	 */
	public String[] get(NodeInfo node, String skey) throws DHTBase.Failed {
		
		UriBuilder ub = UriBuilder.fromUri(node.addr); 
		URI n = ub.queryParam("key", skey).build(); 
		info("client getKeyValuePath(" + n + ")"); 
		Response response = getRequest(n); 
		
		if (response == null || response.getStatus() >= 300) {
			throw new DHTBase.Failed("Get /key?=KEY");
		}
		else { 
			String [] value = response.readEntity(String[].class); 
			return value; 
		}
	}

	// TODO 
	/*
	 * Put bindings under a key.
	 */
	
	//test test test FIXME
	public void add(NodeInfo node, String skey, String v) throws DHTBase.Failed {
		
		UriBuilder ub = UriBuilder.fromUri(node.addr); 
		
		URI xx = ub.queryParam("key", skey).queryParam("value", v).build(); 
		info("client add (" + xx + ")"); 
		Response response = putRequest(xx); 
		
		if (response == null || response.getStatus() >= 300) {
			throw new DHTBase.Failed("Add /add?=value");
		}
	}

	// TODO 
	/*
	 * Delete bindings under a key.
	 */
	public void delete(NodeInfo node, String skey, String v) throws DHTBase.Failed {
	
		UriBuilder ub = UriBuilder.fromUri(node.addr).path("delete"); 
		URI xxx = ub.queryParam("key", skey).queryParam("value", v).build(); 
		info("client delete (" + xxx + ")");
		Response response = deleteRequest(xxx); 
		
		if (response == null || response.getStatus() >= 300) {
			throw new DHTBase.Failed("DELETE /delete?id=ID"); 
		}
	}

	// TODO 
	/*
	 * Find successor of an id. Used by join protocol
	 */
	public NodeInfo findSuccessor(URI addr, int id) throws DHTBase.Failed {
		
		UriBuilder ub = UriBuilder.fromUri(addr).path("succ"); 
		URI xxxx = ub.queryParam("id", id).build(); 
		info("client findSuccessor (" + xxxx + ")");
		Response response = getRequest(xxxx);
		
		if (response == null || response.getStatus() >= 300) {
			throw new DHTBase.Failed("GET /findSuccesor?id=ID"); 
	}
		else { 
	return response.readEntity(NodeInfo.class); 
	
		}
	}
}
