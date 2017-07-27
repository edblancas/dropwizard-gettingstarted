package edblancas.resources;

import edblancas.api.Game;
import edblancas.db.GameDao;

import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by dan on 20/06/17.
 */
@Path("/game")
public class GameResource {
    private static final Logger LOGGER = LoggerFactory.getLogger(GameResource.class);
    private final GameDao gameDao;

    public GameResource(GameDao gameDao) {
        this.gameDao = gameDao;
    }

    @GET
    @Path("/{brand}/{gameId}")
    @Consumes({"application/json", "application/xml"})
    @Produces({"application/json", "application/xml"})
    public Response getGameByBrandAndGameId(@PathParam("brand") final String brand,
                                            @PathParam("gameId") final Long gameId) {
        Response response;
        try {
            Game game = new Game(brand, gameId);
            game = gameDao.get(game.getKey());
            if (game != null) {
                response = Response.status(Response.Status.OK).entity(game).build();
            } else {
                response = Response.status(Response.Status.NOT_FOUND).entity(null).build();
            }
        } catch (IOException e) {
            LOGGER.error("Error querying the provider.", e);
            response = Response.status(500).build();
            e.printStackTrace();
        }

        return response;
    }
}
