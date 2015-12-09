package vertx.playground.java.api;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.UpdateResult;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.ext.jdbc.JDBCClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import vertx.playground.java.util.Runner;

public class VertxRestServer extends AbstractVerticle {

	private static final Log log = LogFactory.getLog(VertxRestServer.class);
	private Map<Integer, Whisky> products = new LinkedHashMap<>();
	private JDBCClient jdbc;

	/**
	 * This method is called when the verticle is deployed. It creates a HTTP
	 * server and registers a simple request handler.
	 * <p/>
	 * Notice the `listen` method. It passes a lambda checking the port binding
	 * result. When the HTTP server has been bound on the port, it call the
	 * `complete` method to inform that the starting has completed. Else it
	 * reports the error.
	 *
	 * @param fut
	 *            the future
	 */
	@Override
	public void start(Future<Void> fut) {

		jdbc = JDBCClient.createShared(vertx, config(), "My-Whisky-Collection");

		startBackend(
				(connection) -> createSomeData(
						connection,
						(nothing) -> startWebApp((http) -> completeStartup(
								http, fut)), fut), fut);

	}

	private void startWebApp(Handler<AsyncResult<HttpServer>> next) {
		// Create a router object.
		Router router = Router.router(vertx);

		// Bind "/" to our hello message.
		router.route("/")
				.handler(
						routingContext -> {
							HttpServerResponse response = routingContext
									.response();
							response.putHeader("content-type", "text/html")
									.end("<h1>Hello from my first Vert.x 3 application</h1>");
						});

		router.route("/assets/*").handler(StaticHandler.create("assets"));

		router.get("/api/whiskies").handler(this::getAll);
		router.route("/api/whiskies*").handler(BodyHandler.create());
		router.post("/api/whiskies").handler(this::addOne);
		router.get("/api/whiskies/:id").handler(this::getOne);
		router.put("/api/whiskies/:id").handler(this::updateOne);
		router.delete("/api/whiskies/:id").handler(this::deleteOne);

		// Create the HTTP server and pass the "accept" method to the request
		// handler.
		vertx.createHttpServer().requestHandler(router::accept).listen(
		// Retrieve the port from the configuration,
		// default to 8081.
				config().getInteger("http.port", 8081), next::handle);
	}

	private void startBackend(Handler<AsyncResult<SQLConnection>> next,
			Future<Void> fut) {
		jdbc.getConnection(ar -> {
			if (ar.failed()) {
				log.error("db connection failed", ar.cause());
				fut.fail(ar.cause());
			} else {
				log.info("db connection succeed");
				next.handle(Future.succeededFuture(ar.result()));
			}
		});
	}
	
	private void startMongodb(Handler<AsyncResult<SQLConnection>> next,
			Future<Void> fut) {
		jdbc.getConnection(ar -> {
			if (ar.failed()) {
				log.error("mongodb connection failed", ar.cause());
				fut.fail(ar.cause());
			} else {
				log.info("mongodb connection succeed");
				next.handle(Future.succeededFuture(ar.result()));
			}
		});
	}

	private void completeStartup(AsyncResult<HttpServer> http, Future<Void> fut) {
		if (http.succeeded()) {
			log.info("Server started succeessfully");
			fut.complete();
		} else {
			log.error("Server start failed", http.cause());
			fut.fail(http.cause());
		}
	}

	private void addOne(RoutingContext routingContext) {
		jdbc.getConnection(ar -> {
			// Read the request's content and create an instance of Whisky.
			final Whisky whisky = Json.decodeValue(
					routingContext.getBodyAsString(), Whisky.class);
			SQLConnection connection = ar.result();
			insert(whisky,
					connection,
					(r) -> {
						if (r.succeeded()) {
							routingContext
									.response()
									.setStatusCode(201)
									.putHeader("content-type",
											"application/json; charset=utf-8")
									.end(Json.encodePrettily(r.result()));

						} else {
							log.error("insert failed");
							routingContext.response().setStatusCode(404).end();
						}
					});
		});

	}

	private void getOne(RoutingContext routingContext) {
		final String id = routingContext.request().getParam("id");
		if (id == null) {
			routingContext.response().setStatusCode(400).end();
		} else {
			jdbc.getConnection(ar -> {
				// Read the request's content and create an instance of Whisky.
				SQLConnection connection = ar.result();
				select(id,
						connection,
						result -> {
							if (result.succeeded()) {
								routingContext
										.response()
										.setStatusCode(200)
										.putHeader("content-type",
												"application/json; charset=utf-8")
										.end(Json.encodePrettily(result
												.result()));
							} else {
								routingContext.response().setStatusCode(404)
										.end();
							}
						});
			});
		}
	}

	private void updateOne(RoutingContext routingContext) {
		final String id = routingContext.request().getParam("id");
		JsonObject json = routingContext.getBodyAsJson();
		if (id == null || json == null) {
			routingContext.response().setStatusCode(400).end();
		} else {
			jdbc.getConnection(ar -> update(
					id,
					json,
					ar.result(),
					(whisky) -> {
						if (whisky.failed()) {
							routingContext.response().setStatusCode(404).end();
						} else {
							routingContext
									.response()
									.putHeader("content-type",
											"application/json; charset=utf-8")
									.end(Json.encodePrettily(whisky.result()));
						}
					}));
		}
	}

	private void deleteOne(RoutingContext routingContext) {
		String id = routingContext.request().getParam("id");
		if (id == null) {
			routingContext.response().setStatusCode(400).end();
		} else {
			jdbc.getConnection(ar -> {
				SQLConnection connection = ar.result();
				connection.execute(
						"DELETE FROM Whisky WHERE id='" + id + "'",
						result -> routingContext
								.response()
								.setStatusCode(204)
								.setStatusMessage(
										"record o f" + id + " is deleted")
								.end());
			});
		}
	}

	private void getAll(RoutingContext routingContext) {
		jdbc.getConnection(ar -> {
			SQLConnection connection = ar.result();
			connection.query(
					"SELECT * FROM Whisky",
					result -> {
						List<Whisky> whiskies = result.result().getRows()
								.stream().map(Whisky::new)
								.collect(Collectors.toList());
						routingContext
								.response()
								.putHeader("content-type",
										"application/json; charset=utf-8")
								.end(Json.encodePrettily(whiskies));
					});
		});
	}

	private void createSomeData(AsyncResult<SQLConnection> result,
			Handler<AsyncResult<Void>> next, Future<Void> fut) {
		if (result.failed()) {
			fut.fail(result.cause());
		} else {
			SQLConnection connection = result.result();
			connection
					.execute(
							"CREATE TABLE IF NOT EXISTS Whisky (id INTEGER IDENTITY, name varchar(100), origin varchar"
									+ "(100))",
							ar -> {
								if (ar.failed()) {
									fut.fail(ar.cause());
									return;
								}
								connection
										.query("SELECT * FROM Whisky",
												select -> {
													if (select.failed()) {
														fut.fail(ar.cause());
														return;
													}
													if (select.result()
															.getNumRows() == 0) {
														insert(new Whisky(
																"Bowmore 15 Years Laimrig",
																"Scotland, Islay"),
																connection,
																(v) -> insert(
																		new Whisky(
																				"Talisker 57° North",
																				"Scotland, Island"),
																		connection,
																		(r) -> next
																				.handle(Future
																						.<Void> succeededFuture())));
													} else {
														next.handle(Future
																.<Void> succeededFuture());
													}
												});

							});
		}
	}

	private void insert(Whisky whisky, SQLConnection connection,
			Handler<AsyncResult<Whisky>> next) {
		String sql = "INSERT INTO Whisky (name, origin) VALUES ?, ?";
		connection.updateWithParams(sql,
				new JsonArray().add(whisky.getName()).add(whisky.getOrigin()),
				(ar) -> {
					if (ar.failed()) {
						next.handle(Future.failedFuture(ar.cause()));
						return;
					}
					UpdateResult result = ar.result();
					// Build a new whisky instance with the generated id.
				Whisky w = new Whisky(result.getKeys().getInteger(0), whisky
						.getName(), whisky.getOrigin());
				next.handle(Future.succeededFuture(w));
			});
	}

	private void select(String id, SQLConnection connection,
			Handler<AsyncResult<Whisky>> resultHandler) {
		connection.queryWithParams("SELECT * FROM Whisky WHERE id=?",
				new JsonArray().add(id), ar -> {
					if (ar.failed()) {
						resultHandler.handle(Future
								.failedFuture("Whisky not found"));
					} else {
						if (ar.result().getNumRows() >= 1) {
							resultHandler.handle(Future
									.succeededFuture(new Whisky(ar.result()
											.getRows().get(0))));
						} else {
							resultHandler.handle(Future
									.failedFuture("Whisky not found"));
						}
					}
				});
	}

	private void update(String id, JsonObject content,
			SQLConnection connection, Handler<AsyncResult<Whisky>> resultHandler) {
		String sql = "UPDATE Whisky SET name=?, origin=? WHERE id=?";
		connection.updateWithParams(
				sql,
				new JsonArray().add(content.getString("name"))
						.add(content.getString("origin")).add(id), update -> {
					if (update.failed()) {
						resultHandler.handle(Future
								.failedFuture("Cannot update the whisky"));
						return;
					}
					if (update.result().getUpdated() == 0) {
						resultHandler.handle(Future
								.failedFuture("Whisky not found"));
						return;
					}
					resultHandler.handle(Future.succeededFuture(new Whisky(
							Integer.valueOf(id), content.getString("name"),
							content.getString("origin"))));
				});
		
	}

	public static void main(String[] args) {

		InputStream in = VertxRestServer.class.getClassLoader()
				.getResourceAsStream("my-application-conf.json");
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		StringBuilder out = new StringBuilder();
		String line;
		try {
			while ((line = reader.readLine()) != null) {
				out.append(line);
			}

			reader.close();
		} catch (IOException e) {

			e.printStackTrace();
		}

		JsonObject config = new JsonObject(out.toString());
		DeploymentOptions options = new DeploymentOptions().setConfig(config);
		Runner.runExample(VertxRestServer.class, options);
	}

}
