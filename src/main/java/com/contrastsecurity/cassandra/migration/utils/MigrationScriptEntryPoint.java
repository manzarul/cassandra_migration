package com.contrastsecurity.cassandra.migration.utils;

import com.contrastsecurity.cassandra.migration.CassandraMigration;
import com.contrastsecurity.cassandra.migration.config.Keyspace;
import com.contrastsecurity.cassandra.migration.logging.Log;
import com.contrastsecurity.cassandra.migration.logging.LogFactory;

public class MigrationScriptEntryPoint {

	private static final Log LOG = LogFactory.getLog(CassandraMigration.class);
	private static final String CASSANDRA__KEYSPACE = "sunbird";//"cassandra_migration_test";
	private static int CASSANDRA_PORT = 9042;
	private static String[] CASSANDRA_HOST;
	private static String CASSANDRA_USER_NAME;
	private static String CASSANDRA_PASSWORD;
	private static final String SUNBIRD_CASSANDRA_PORT = "sunbird_cassandra_port";
	private static final String SUNBIRD_CASSANDRA_HOST = "sunbird_cassandra_host";
	private static final String SUNBIRD_CASSANDRA_USERNAME = "sunbird_cassandra_username";
	private static final String SUNBIRD_CASSANDRA_PASSWORD = "sunbird_cassandra_password";
	private static final String[] SCRIPT_LOCATIONS = { "db/migration/cassandra" };

	/**
	 * main method to run cassandra migration; |
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		LOG.info("Migration started at ==" + System.currentTimeMillis());
		System.out.println("Migration started at ==" + System.currentTimeMillis());
		try {
			init();
			Keyspace keyspace = createSpaces();
			CassandraMigration cm = new CassandraMigration();
			cm.getConfigs().setScriptsLocations(SCRIPT_LOCATIONS);
			cm.setKeyspace(keyspace);
			cm.migrate();
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("Exception occured during migration", e);
		}
		LOG.info("Migration Completed at ==" + System.currentTimeMillis());
		System.out.println("Migration Completed at ==" + System.currentTimeMillis());
	}

	/**
	 * This method will initialize cassandra db connection configuration values.
	 * 
	 * @throws Exception
	 */
	public static void init() throws Exception {
		String host = System.getenv(SUNBIRD_CASSANDRA_HOST);
		String port = System.getenv(SUNBIRD_CASSANDRA_PORT);
		String userName = System.getenv(SUNBIRD_CASSANDRA_USERNAME);
		String password = System.getenv(SUNBIRD_CASSANDRA_PASSWORD);
		if (host == null || port == null || "".equals(host.trim()) || "".equals(port.trim())) {
			Exception e = new Exception("Cassandra configuration values are not set");
			LOG.error("Exception occured during migration", e);
			throw e;
		} else {
			CASSANDRA_HOST = host.split(",");
			CASSANDRA_PORT = Integer.parseInt(port.split(",")[0]);
			CASSANDRA_USER_NAME = userName;
			CASSANDRA_PASSWORD = password;
		}
	}

	/**
	 * This method will create keyspace
	 * @return Keyspace
	 */
	public static Keyspace createSpaces() {
		Keyspace keyspace = new Keyspace();
		keyspace.setName(CASSANDRA__KEYSPACE);
		keyspace.getCluster().setContactpoints(CASSANDRA_HOST);
		keyspace.getCluster().setPort(CASSANDRA_PORT);
		if (CASSANDRA_USER_NAME != null && !"".equals(CASSANDRA_USER_NAME.trim()) && CASSANDRA_PASSWORD != null
				&& !"".equals(CASSANDRA_PASSWORD.trim())) {
			keyspace.getCluster().setUsername(CASSANDRA_USER_NAME);
			keyspace.getCluster().setPassword(CASSANDRA_PASSWORD);
		}
		return keyspace;
	}

}
