package edblancas.api;

import org.hibernate.validator.constraints.NotBlank;

import javax.validation.constraints.NotNull;
import java.io.Serializable;

/**
 * Created by dan on 20/06/17.
 */
public class Game implements Serializable {
    private static final long serialVersionUID = -868812659975664018L;
    private Key key = new Key();
    private String name;
    private String console;
    private String usd;
    private String mxn;

    public Game() {
    }

    public Game(final String brand, final long gameId, final String name, final String console, final String usd, final String mxn) {
        this.key.setBrand(brand);
        this.key.setGameId(gameId);
        this.name = name;
        this.console = console;
        this.usd = usd;
        this.mxn = mxn;
    }

    public Game(final String brand, final Long gameId) {
        this.key.setBrand(brand);
        this.key.setGameId(gameId);
    }

    public String getBrand() {
        return key.getBrand();
    }

    public void setBrand(final String brand) {
        key.setBrand(brand);
    }

    public long getGameId() {
        return key.getGameId();
    }

    public void setGameId(final long gameId) {
        key.setGameId(gameId);
    }

    public Key getKey() {
        return key;
    }

    public void setKey(final Key key) {
        this.key = key;
    }

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public String getConsole() {
        return console;
    }

    public void setConsole(final String console) {
        this.console = console;
    }

    public String getUsd() {
        return usd;
    }

    public void setUsd(final String usd) {
        this.usd = usd;
    }

    public String getMxn() {
        return mxn;
    }

    public void setMxn(final String mxn) {
        this.mxn = mxn;
    }

    @Override
    public String toString() {
        return "Game{" +
                "key=" + key +
                ", name='" + name + '\'' +
                ", console='" + console + '\'' +
                ", usd='" + usd + '\'' +
                ", mxn='" + mxn + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Game game = (Game) o;

        if (!key.equals(game.key)) return false;
        if (!name.equals(game.name)) return false;
        if (!console.equals(game.console)) return false;
        if (usd != null ? !usd.equals(game.usd) : game.usd != null) return false;
        return mxn != null ? mxn.equals(game.mxn) : game.mxn == null;
    }

    @Override
    public int hashCode() {
        int result = key.hashCode();
        result = 31 * result + name.hashCode();
        result = 31 * result + console.hashCode();
        result = 31 * result + (usd != null ? usd.hashCode() : 0);
        result = 31 * result + (mxn != null ? mxn.hashCode() : 0);
        return result;
    }

    public static class Key implements Serializable {
        private static final long serialVersionUID = 1829955187190259144L;
        @NotBlank
        @NotNull
        private String brand;
        private long gameId;

        public Key() {
        }

        public Key(String brand, long gameId) {
//            super();
            this.brand = brand;
            this.gameId = gameId;
        }

        public String getBrand() {
            return brand;
        }

        public void setBrand(String brand) {
            this.brand = brand;
        }

        public long getGameId() {
            return gameId;
        }

        public void setGameId(long gameId) {
            this.gameId = gameId;
        }

        @Override
        public String toString() {
            return "Key{" +
                    "brand='" + brand + '\'' +
                    ", gameId=" + gameId +
                    '}';
        }
    }
}
