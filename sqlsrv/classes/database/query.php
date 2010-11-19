<?php defined('SYSPATH') or die('No direct script access.');
/**
 * Database query wrapper.
 * 
 * MS SQL Server extension to allow parameter extraction
 * before binding
 *
 * @package    Kohana/Database
 * @category   Query
 * @author     Kohana Team
 * @copyright  (c) 2008-2009 Kohana Team
 * @license    http://kohanaphp.com/license
 */
class Database_Query extends Kohana_Database_Query {

	/**
	 * @var     string
	 */
	protected $_rendered_sql;

	/**
	 * Returns the parameters set to this query object
	 *
	 * @return  array
	 */
	public function get_parameters()
	{
		return $this->_parameters;
	}

	/**
	 * Compile the SQL query and return it. Replaces any parameters with their
	 * given values.
	 *
	 * @param   object  Database instance
	 * @return  string
	 * @return  statement resource
	 */
	public function compile(Database $db)
	{
		// If this is not a Sqlsrv database return the standard logic
		if ( ! $db instanceof Database_Sqlsrv)
			return parent::compile($db);

		// Import the sql locally
		$sql = $this->_sql;

		// Get the Kohana rendered SQL for caching purposes
		$this->_rendered_sql = parent::compile($db);

		// Turn Kohana parameters into SQL Server parameters
		return preg_replace('/(:\w+)/', '?', $sql);
	}


	/**
	 * Execute the current query on the given database.
	 *
	 * @param   mixed    Database instance or name of instance
	 * @return  object   Database_Result for SELECT queries
	 * @return  mixed    the insert id for INSERT queries
	 * @return  integer  number of affected rows for all other queries
	 */
	public function execute($db = NULL)
	{
		if ( ! is_object($db))
		{
			// Get the database instance
			$db = Database::instance($db);
		}

		// If the db is not a Sqlsrv database, use the parent method
		if ( ! $db instanceof Database_Sqlsrv)
			return parent::execute($db);

		// Compile the SQL into a SQL Server Statement resource
		$sql = $this->compile($db);

		if ( ! empty($this->_lifetime) AND $this->_type === Database::SELECT)
		{
			// Set the cache key based on the database instance name and SQL
			$cache_key = 'Database::query("'.$db.'", "'.$this->_rendered_sql.'")';

			if ($result = Kohana::cache($cache_key, NULL, $this->_lifetime))
			{
				// Return a cached result
				return new Database_Result_Cached($result, $_rendered_sql, $this->_as_object, $this->_object_params);
			}
		}

		// Clean the params or create a NULL value
		if (is_array($this->_object_params))
			$params = array_values($this->_object_params);
		else
			$params = NULL;

		// Execute the query
		$result = $db->query($this->_type, $sql, $this->_as_object, $params);

		if (isset($cache_key))
		{
			// Cache the result array
			Kohana::cache($cache_key, $result->as_array(), $this->_lifetime);
		}

		return $result;
	}
}