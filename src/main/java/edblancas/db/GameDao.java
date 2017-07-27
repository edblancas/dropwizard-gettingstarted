package edblancas.db;

import edblancas.api.Game;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by dan on 20/06/17.
 */
public class GameDao extends AbstractHBaseDAO<Game.Key, Game> {
    private static final Logger LOGGER = LoggerFactory.getLogger(GameDao.class);

    public GameDao(final Table table, final Table reverseIndex, final Table countersTable) {
        super(table, reverseIndex, countersTable);
    }

    public GameDao(Table table) {
        super(table);
    }

    @Override
    protected Put createPut(Game object) throws IOException {
        return null;
    }

    @Override
    protected byte[] createRowKey(Game.Key rowKey) {
        LOGGER.debug("Creating row key for brand {} and gameId {}", rowKey.getBrand(), rowKey.getGameId());
//        final int brandByteLength = rowKey.getBrand().getBytes().length;
//        final byte[] rowKeyArray = new byte[brandByteLength + Long.BYTES];
//        System.arraycopy(Bytes.toBytes(rowKey.getBrand()), 0, rowKeyArray, 0, brandByteLength);
//        System.arraycopy(Bytes.toBytes(rowKey.getGameId()), 0, rowKeyArray, brandByteLength, Long.BYTES);
        final byte[] rowKeyArray = Bytes.toBytes(rowKey.getBrand() + rowKey.getGameId());
        return rowKeyArray;
    }

    @Override
    protected byte[] createRowKeyFromObject(Game object) {
        return new byte[0];
    }

    @Override
    protected byte[] createReverseRowKey(Game.Key rowKey) {
        return new byte[0];
    }

    @Override
    protected byte[] createReverseRowKeyFromObject(Game object) {
        return new byte[0];
    }

    @Override
    protected Game createObject(Result row) {
        final Game game = new Game();
        game.setName(Bytes.toString(row.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"))));
        return game;
    }

    @Override
    protected PrefixFilter createPrefixFilter(Game.Key rowKey) {
        return null;
    }
}
