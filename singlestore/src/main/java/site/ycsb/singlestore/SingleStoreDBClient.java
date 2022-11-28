package site.ycsb.singlestore;

import site.ycsb.*;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.singlestore.jdbc.SingleStorePoolDataSource;

import java.util.concurrent.atomic.AtomicInteger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * SingleStore client for YCSB framework.
 *
 * We do not use PreparedStatement because Singlestore automatically compiles queries and stores
 * them in the plancache.
 */
public class SingleStoreDBClient extends DB {
  private static final Logger LOG = LoggerFactory.getLogger(SingleStoreDBClient.class);

  /** Count the number of times initialized to teardown on the last. */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  /** The connection to the database. */
  private static Connection connection;

  /** Resource pool for database connections. */
  private static SingleStorePoolDataSource pool;

  /** The URL to connect to the database. */
  public static final String CONNECTION_URL = "singlestore.url";

  /** The user name to use to connect to the database. */
  public static final String CONNECTION_USER = "singlestore.user";

  /** The password to use for establishing the connection. */
  public static final String CONNECTION_PASSWD = "singlestore.passwd";

  /** Whether or not to use JSON_SET in update queries. */
  public static final String USE_JSON_SET = "singlestore.use_json_set";

  /** The maximum size of the connection pool. */
  public static final String MAX_POOOL_SIZE = "singlestore.max_pool_size";

  /** The primary key in the user table. */
  public static final String PRIMARY_KEY = "YCSB_KEY";

  /** The field name prefix in the table. */
  public static final String COLUMN_NAME = "YCSB_VALUE";

  private static final String DEFAULT_PROP = "";

  /** Returns parsed boolean value from the properties if set, otherwise returns defaultVal. */
  private static boolean getBoolProperty(Properties props, String key, boolean defaultVal) {
    String valueStr = props.getProperty(key);
    if (valueStr != null) {
      return Boolean.parseBoolean(valueStr);
    }
    return defaultVal;
  }

  /**
   * Wraps a literal in single-quotes and escapes all inner single-quotes.
   */
  private static String literalToSQLString(String literal) {
    return "'" + literal.replace("\\", "\\\\").replace("'", "\\'") + "'";
  }

  /** 
   * Becasue the current implementation of JSON_SET in SingleStore is often not as efficient for
   * performing multi-field updates this flag is set to determine if JSON_SET should be used.
   */
  private boolean useJsonSet;

  @Override
  public void init() throws DBException {
    INIT_COUNT.incrementAndGet();
    synchronized (SingleStoreDBClient.class) {
      if (pool != null) {
        return;
      }
      Properties props = getProperties();
      String urls = props.getProperty(CONNECTION_URL, DEFAULT_PROP);
      String user = props.getProperty(CONNECTION_USER, DEFAULT_PROP);
      String passwd = props.getProperty(CONNECTION_PASSWD, DEFAULT_PROP);
      useJsonSet = getBoolProperty(props, USE_JSON_SET, false);
      String maxPoolSize = props.getProperty(MAX_POOOL_SIZE, "1");

      try {
        Properties tmpProps = new Properties();
        tmpProps.setProperty("user", user);
        tmpProps.setProperty("password", passwd);

        pool = new SingleStorePoolDataSource(urls + "?user=" + user + "&password=" + passwd +
            "&maxPoolSize=" + maxPoolSize + "&autocommit=false");
        
      } catch (Exception e) {
        LOG.error("Error during initialization: " + e);
      }
    }
  }

  @Override
  public void cleanup() throws DBException {
    if (INIT_COUNT.decrementAndGet() == 0) {
      try (Connection connection = pool.getConnection();
        Statement stmt = connection.createStatement()) {
        connection.commit();
      } catch (SQLException e) {
        LOG.error("Error in processing commit");
      }
      pool.close();
    }
  }

  @Override
  public Status read(String tableName, String key, Set<String> fields, Map<String, ByteIterator> result) {
    StringBuilder readQuery = new StringBuilder("SELECT " + PRIMARY_KEY + " AS " + PRIMARY_KEY);
    if (fields == null) {
      readQuery.append(", " + COLUMN_NAME + " AS " + COLUMN_NAME);
    } else {
      for (String field : fields) {
        readQuery.append(", JSON_EXTRACT_STRING(" + COLUMN_NAME + ", " + literalToSQLString(field) + ") as " + field);
      }
    }
    readQuery.append(" FROM ");
    readQuery.append(tableName);
    readQuery.append(" WHERE ");
    readQuery.append(PRIMARY_KEY);
    readQuery.append(" = ");
    readQuery.append(literalToSQLString(key));

    try (Connection connection = pool.getConnection();
      Statement stmt = connection.createStatement()) {

      stmt.setFetchSize(1);
      
      ResultSet resultSet = stmt.executeQuery(readQuery.toString());
      if (!resultSet.next()) {
        resultSet.close();
        return  Status.NOT_FOUND;
      }

      if (result != null) {
        if (fields == null) {
          String jsonResult = resultSet.getString(COLUMN_NAME);
          JSONObject jsonObject = (JSONObject) JSONValue.parse(jsonResult);

          Iterator<String> iterator = jsonObject.keySet().iterator();
          while(iterator.hasNext()) {
            String field = iterator.next();
            String value = jsonObject.get(field).toString();
            result.put(field, new StringByteIterator(value));
          }
        } else {
          for (String field : fields) {
            String value = resultSet.getString(field);
            result.put(field, new StringByteIterator(value));
          }
        }
      }
      resultSet.close();
      return Status.OK;
    } catch (SQLException e) {
      LOG.error("Error in processing read of table: " + tableName + ": " + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String tableName, String startKey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    StringBuilder scanQuery = new StringBuilder("SELECT " + PRIMARY_KEY + " AS " + PRIMARY_KEY);
    if (fields == null) {
      scanQuery.append(", " + COLUMN_NAME + " AS " + COLUMN_NAME);
    } else {
      for (String field : fields) {
        scanQuery.append(", JSON_EXTRACT_STRING(" + COLUMN_NAME + ", " + literalToSQLString(field) + ") as " + field);
      }
    }
    scanQuery.append(" FROM ");
    scanQuery.append(tableName);
    scanQuery.append(" WHERE ");
    scanQuery.append(PRIMARY_KEY);
    scanQuery.append(" >= ");
    scanQuery.append(literalToSQLString(startKey));
    scanQuery.append(" ORDER BY ");
    scanQuery.append(PRIMARY_KEY);
    scanQuery.append(" LIMIT " + recordcount);

    try (Connection connection = pool.getConnection();
      Statement stmt = connection.createStatement()) {
      stmt.setFetchSize(recordcount);
      ResultSet resultSet = stmt.executeQuery(scanQuery.toString());
      int i = 0;
      for (; i < recordcount && resultSet.next(); ++i) {
        if (result != null) {
          if (fields == null) {
            String jsonResult = resultSet.getString(COLUMN_NAME);
            JSONObject jsonObject = (JSONObject) JSONValue.parse(jsonResult);

            HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
            Iterator<String> iterator = jsonObject.keySet().iterator();
            while(iterator.hasNext()) {
              String field = iterator.next();
              String value = jsonObject.get(field).toString();
              values.put(field, new StringByteIterator(value));
            }
            result.add(values);
          } else {
            HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
            for (String field : fields) {
              String value = resultSet.getString(field);
              values.put(field, new StringByteIterator(value));
            }
            result.add(values);
          }
        }
      }

      resultSet.close();
      if (i == 0 && recordcount > 0) {
        return  Status.NOT_FOUND;
      }
      return Status.OK;
    } catch (SQLException e) {
      LOG.error("Error in processing scan of table: " + tableName + ": " + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String tableName, String key, Map<String, ByteIterator> values) {

    if (useJsonSet) {
      // For each key-value pair in values, peform an update query using JSON_SET.
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        StringBuilder updateQuery = new StringBuilder("UPDATE " + tableName);
        updateQuery.append(" SET " + COLUMN_NAME + " = ");
        updateQuery.append("JSON_SET_STRING(");
        updateQuery.append("VALUES(" + COLUMN_NAME + "),");
        updateQuery.append(literalToSQLString(entry.getKey()) + ",");
        updateQuery.append(literalToSQLString(entry.getValue().toString()));
        updateQuery.append(")");
        updateQuery.append(" WHERE " + PRIMARY_KEY + " = " + literalToSQLString(key));
        try (Connection connection = pool.getConnection();
          Statement stmt = connection.createStatement()) {
          int result = stmt.executeUpdate(updateQuery.toString());
          connection.commit();
        } catch (SQLException e) {
          LOG.error("Error when updating a path(useJsonSet):{},tableName:{}", key, tableName, e);
          return Status.ERROR;
        }
      }
      return Status.OK;
    } else {
      // Peform the update in two steps: read, update entire json value.
      StringBuilder readQuery = new StringBuilder("SELECT " + COLUMN_NAME + " AS " + COLUMN_NAME);
      readQuery.append(" FROM " + tableName);
      readQuery.append(" WHERE " + PRIMARY_KEY + "=" + literalToSQLString(key));
      try (Connection connection = pool.getConnection();
        Statement stmt = connection.createStatement()) {
        ResultSet resultSet = stmt.executeQuery(readQuery.toString());
        if (!resultSet.next()) {
          resultSet.close();
          return  Status.NOT_FOUND;
        }
        String jsonResult = resultSet.getString(COLUMN_NAME);
        JSONObject jsonObject = (JSONObject) JSONValue.parse(jsonResult);
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
          jsonObject.put(entry.getKey(), entry.getValue().toString());
        }
        resultSet.close();

        StringBuilder updateQuery = new StringBuilder("UPDATE " + tableName);
        updateQuery.append(" SET " + COLUMN_NAME + " = ");
        updateQuery.append(literalToSQLString(jsonObject.toJSONString()));
        updateQuery.append(" WHERE " + PRIMARY_KEY + " = " + literalToSQLString(key));

        int result = stmt.executeUpdate(updateQuery.toString());
        connection.commit();
        return Status.OK;
      } catch (SQLException e) {
        LOG.error("Error when updating a path:{},tableName:{}", key, tableName, e);
        return Status.ERROR;
      }
    }
  }

  @Override
  public Status insert(String tableName, String key, Map<String, ByteIterator> values) {
    StringBuilder insertQuery = new StringBuilder("INSERT INTO " + tableName);
    insertQuery.append(" (" + PRIMARY_KEY + "," + COLUMN_NAME + ")");
    insertQuery.append(" VALUES (" + literalToSQLString(key) + ", JSON_BUILD_OBJECT(");

    int i = 0;
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      if (i > 0) {
        insertQuery.append(",");
      }
      insertQuery.append(literalToSQLString(entry.getKey()) + "," + literalToSQLString(entry.getValue().toString()));
      ++i;
    }
    insertQuery.append("))");

    try (Connection connection = pool.getConnection();
      Statement stmt = connection.createStatement()) {
      int result = stmt.executeUpdate(insertQuery.toString());
      connection.commit();
      if (result == 1) {
        return Status.OK;
      }

      return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      LOG.error("Error in processing insert to table: " + tableName + ": " + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String tableName, String key) {
    StringBuilder deleteQuery = new StringBuilder("DELETE FROM " + tableName);
    deleteQuery.append(" WHERE " + PRIMARY_KEY + "=" + literalToSQLString(key));

    try (Connection connection = pool.getConnection();
      Statement stmt = connection.createStatement()) {
      int result = stmt.executeUpdate(deleteQuery.toString());
      connection.commit();
      if (result == 1) {
        return Status.OK;
      }

      return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      LOG.error("Error in processing delete to table: " + tableName + e);
      return Status.ERROR;
    }
  }
};
