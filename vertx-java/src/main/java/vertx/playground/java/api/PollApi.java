package vertx.playground.java.api;

import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.StaticHandler;

public class PollApi {
	
	
	public void startPoll(Router router){
		

	    router.get("/api/polls").handler(this::getPoll);
	    router.route("/api/polls*").handler(BodyHandler.create());

		
		
		
	}
	
	private void getPoll(RoutingContext routingContext){
		
	}
	

}
